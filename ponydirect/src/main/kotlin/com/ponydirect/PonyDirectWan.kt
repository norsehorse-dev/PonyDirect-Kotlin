package com.ponydirect

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * The WAN path manager: one shared UDP socket, per-peer hole-punch sessions.
 * Gathers host + STUN-reflexive candidates, exchanges them over the app's sealed
 * signaling, punches with authenticated PROBE/PONG, and holds the mapping with
 * KEEPALIVE. M2 proves an authenticated path; envelope delivery is M3. The state
 * machine mirrors PonyDirectWan (Swift).
 */
class PonyDirectWan(
    private val stun: StunServer,
    private val keys: PonyDirectKeyProvider,
    private val signaling: PonyDirectSignaling,
) {

    enum class PathState { IDLE, GATHERING, PUNCHING, CONNECTED, FAILED }
    enum class Role { INITIATOR, RESPONDER }

    data class StunServer(val host: String, val port: Int)

    interface Delegate {
        fun onPathState(peerID: String, state: PathState)
        fun onPayload(peerID: String, payload: ByteArray)
    }

    var delegate: Delegate? = null

    private class Session(val peerID: String, val role: Role, var sessionNonce: ByteArray) {
        var remoteCandidates: MutableList<String> = ArrayList()
        var activeRemote: PonyDirectUdpSocket.Source? = null
        var state: PathState = PathState.IDLE
        var probeRounds = 0
    }

    private val socket = PonyDirectUdpSocket()
    private val exec = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "ponydirect-wan").apply { isDaemon = true }
    }
    private val sessions = ConcurrentHashMap<String, Session>()
    @Volatile private var stunTxn: ByteArray? = null
    @Volatile private var reflexive: String? = null
    private var hostCandidates: List<String> = emptyList()
    private var probeTask: ScheduledFuture<*>? = null
    private var keepaliveTask: ScheduledFuture<*>? = null

    private val probeIntervalMs = 250L
    private val maxProbeRounds = 40
    private val keepaliveIntervalMs = 15_000L

    init {
        socket.onDatagram = { data, source -> exec.execute { handleDatagram(data, source) } }
        socket.start()
    }

    /** Open a path to [peerID]. Initiator mints the session nonce and sends the offer. */
    fun open(peerID: String, role: Role) = exec.execute {
        if (sessions.containsKey(peerID)) return@execute
        val nonce = if (role == Role.INITIATOR) PonyDirectWire.randomBytes(16) else ByteArray(0)
        val session = Session(peerID, role, nonce)
        sessions[peerID] = session
        setState(session, PathState.GATHERING)
        gatherCandidates()
        if (role == Role.INITIATOR) sendOffer(session)
    }

    fun close(peerID: String) = exec.execute {
        val s = sessions.remove(peerID) ?: return@execute
        setState(s, PathState.IDLE)
        if (sessions.isEmpty()) stopTimers()
    }

    fun handleSignal(signal: PonyDirectSignal, peerID: String) = exec.execute {
        when (signal.kind) {
            PonyDirectSignal.Kind.OFFER -> {
                val session = sessions.getOrPut(peerID) {
                    Session(peerID, Role.RESPONDER, signal.sessionNonce ?: ByteArray(0)).also {
                        setState(it, PathState.GATHERING); gatherCandidates()
                    }
                }
                signal.sessionNonce?.let { session.sessionNonce = it }
                session.remoteCandidates = merge(session.remoteCandidates, signal.candidates)
                sendAnswer(session)
                startPunching(session)
            }
            PonyDirectSignal.Kind.ANSWER -> {
                val session = sessions[peerID] ?: return@execute
                signal.sessionNonce?.let { session.sessionNonce = it }
                session.remoteCandidates = merge(session.remoteCandidates, signal.candidates)
                startPunching(session)
            }
            PonyDirectSignal.Kind.ICE -> {
                val session = sessions[peerID] ?: return@execute
                val c = signal.candidate ?: return@execute
                session.remoteCandidates = merge(session.remoteCandidates, listOf(c))
                startPunching(session)
            }
        }
    }

    // Candidate gathering ---------------------------------------------------

    private fun gatherCandidates() {
        if (hostCandidates.isEmpty()) {
            hostCandidates = PonyDirectUdpSocket.hostIPv4Addresses().map { "$it:${socket.localPort}" }
        }
        if (reflexive == null && stunTxn == null) {
            val txn = PonyDirectStun.newTransactionID()
            stunTxn = txn
            socket.send(PonyDirectStun.bindingRequest(txn), stun.host, stun.port)
            exec.schedule({
                if (reflexive == null) { stunTxn = null; flushPending() }
            }, 1, TimeUnit.SECONDS)
        }
    }

    private fun localCandidates(): List<String> {
        val all = ArrayList(hostCandidates)
        reflexive?.let { all.add(it) }
        return all
    }

    private fun sendOffer(session: Session) {
        signaling.sendSignal(
            PonyDirectSignal(PonyDirectSignal.Kind.OFFER, localCandidates(), session.sessionNonce), session.peerID
        )
    }

    private fun sendAnswer(session: Session) {
        signaling.sendSignal(
            PonyDirectSignal(PonyDirectSignal.Kind.ANSWER, localCandidates(), session.sessionNonce), session.peerID
        )
    }

    private fun flushPending() {
        for (s in sessions.values) {
            when (s.role) {
                Role.INITIATOR -> if (s.state == PathState.GATHERING) sendOffer(s)
                Role.RESPONDER -> if (s.state == PathState.GATHERING) sendAnswer(s)
            }
        }
    }

    // Punching --------------------------------------------------------------

    private fun startPunching(session: Session) {
        if (session.state != PathState.GATHERING && session.state != PathState.PUNCHING) return
        if (session.state != PathState.PUNCHING) setState(session, PathState.PUNCHING)
        ensureProbeTimer()
    }

    private fun ensureProbeTimer() {
        if (probeTask != null) return
        probeTask = exec.scheduleAtFixedRate({ probeTick() }, 0, probeIntervalMs, TimeUnit.MILLISECONDS)
    }

    private fun probeTick() {
        var anyPunching = false
        for (session in sessions.values) {
            if (session.state != PathState.PUNCHING) continue
            anyPunching = true
            val pairKey = keys.pairKey(session.peerID) ?: continue
            session.probeRounds++
            if (session.probeRounds > maxProbeRounds) { setState(session, PathState.FAILED); continue }
            for (cand in session.remoteCandidates) {
                val hp = splitHostPort(cand) ?: continue
                val probeNonce = PonyDirectWire.randomBytes(16)
                socket.send(PonyDirectPunch.probe(pairKey, session.sessionNonce, probeNonce), hp.first, hp.second)
            }
        }
        if (!anyPunching) stopProbeTimer()
    }

    private fun stopProbeTimer() { probeTask?.cancel(false); probeTask = null }

    // Datagram intake -------------------------------------------------------

    private fun handleDatagram(data: ByteArray, source: PonyDirectUdpSocket.Source) {
        val txn = stunTxn
        if (txn != null) {
            val mapped = PonyDirectStun.parseResponse(data, txn)
            if (mapped != null) {
                stunTxn = null
                reflexive = "${mapped.ip}:${mapped.port}"
                for (s in sessions.values) {
                    signaling.sendSignal(PonyDirectSignal(PonyDirectSignal.Kind.ICE, candidate = reflexive), s.peerID)
                }
                flushPending()
                return
            }
        }
        val parsed = PonyDirectPunch.parse(data) ?: return
        val session = sessions.values.firstOrNull {
            PonyDirectWire.constantTimeEquals(it.sessionNonce, parsed.sessionNonce)
        } ?: return
        val pairKey = keys.pairKey(session.peerID) ?: return

        when (parsed.type) {
            PonyDirectPunch.PROBE -> {
                if (!PonyDirectPunch.verifyProbe(parsed, pairKey)) return
                val pong = PonyDirectPunch.pong(pairKey, session.sessionNonce, parsed.probeNonce)
                socket.send(pong, source.host, source.port)
                markConnected(session, source)
            }
            PonyDirectPunch.PONG -> {
                if (!PonyDirectPunch.verifyPong(parsed, pairKey)) return
                markConnected(session, source)
            }
            PonyDirectPunch.KEEPALIVE -> { /* NAT-hold only */ }
        }
    }

    private fun markConnected(session: Session, remote: PonyDirectUdpSocket.Source) {
        session.activeRemote = remote
        if (session.state != PathState.CONNECTED) setState(session, PathState.CONNECTED)
        ensureKeepalive()
        if (sessions.values.none { it.state == PathState.PUNCHING }) stopProbeTimer()
    }

    // Keepalive -------------------------------------------------------------

    private fun ensureKeepalive() {
        if (keepaliveTask != null) return
        keepaliveTask = exec.scheduleAtFixedRate(
            { keepaliveTick() }, keepaliveIntervalMs, keepaliveIntervalMs, TimeUnit.MILLISECONDS
        )
    }

    private fun keepaliveTick() {
        for (session in sessions.values) {
            if (session.state != PathState.CONNECTED) continue
            val r = session.activeRemote ?: continue
            socket.send(PonyDirectPunch.keepalive(session.sessionNonce), r.host, r.port)
        }
    }

    private fun stopTimers() {
        stopProbeTimer()
        keepaliveTask?.cancel(false); keepaliveTask = null
    }

    // Public helpers --------------------------------------------------------

    /** Send an application payload over an established path (M3 delivery). */
    fun sendPayload(payload: ByteArray, peerID: String): Boolean {
        val s = sessions[peerID] ?: return false
        val r = s.activeRemote ?: return false
        if (s.state != PathState.CONNECTED) return false
        socket.send(PonyDirectWire.frame(PonyDirectWire.ENVELOPE, payload), r.host, r.port)
        return true
    }

    fun stateOf(peerID: String): PathState = sessions[peerID]?.state ?: PathState.IDLE

    private fun setState(session: Session, state: PathState) {
        session.state = state
        delegate?.onPathState(session.peerID, state)
    }

    private fun merge(a: MutableList<String>, b: List<String>): MutableList<String> {
        val seen = HashSet(a)
        for (c in b) if (seen.add(c)) a.add(c)
        return a
    }

    private fun splitHostPort(s: String): Pair<String, Int>? {
        val idx = s.lastIndexOf(':')
        if (idx <= 0) return null
        val host = s.substring(0, idx)
        val port = s.substring(idx + 1).toIntOrNull() ?: return null
        return host to port
    }
}
