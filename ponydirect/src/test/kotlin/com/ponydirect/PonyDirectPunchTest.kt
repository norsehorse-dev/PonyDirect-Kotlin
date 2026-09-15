package com.ponydirect

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PonyDirectPunchTest {

    @Test fun probeRoundTripAndAuth() {
        val key = ByteArray(32) { 5.toByte() }
        val session = ByteArray(16) { 1.toByte() }
        val nonce = ByteArray(16) { 2.toByte() }
        val pkt = PonyDirectPunch.probe(key, session, nonce)
        assertEquals(PonyDirectPunch.PROBE_LEN, pkt.size)
        val parsed = PonyDirectPunch.parse(pkt); assertNotNull(parsed)
        assertEquals(PonyDirectPunch.PROBE, parsed!!.type)
        assertTrue(session.contentEquals(parsed.sessionNonce))
        assertTrue(nonce.contentEquals(parsed.probeNonce))
        assertTrue(PonyDirectPunch.verifyProbe(parsed, key))
        assertFalse(PonyDirectPunch.verifyProbe(parsed, ByteArray(32) { 9.toByte() }))
        assertFalse(PonyDirectPunch.verifyPong(parsed, key))
    }

    @Test fun pongRoundTripAndAuth() {
        val key = ByteArray(32) { 6.toByte() }
        val session = ByteArray(16) { 3.toByte() }
        val nonce = ByteArray(16) { 4.toByte() }
        val parsed = PonyDirectPunch.parse(PonyDirectPunch.pong(key, session, nonce))!!
        assertEquals(PonyDirectPunch.PONG, parsed.type)
        assertTrue(PonyDirectPunch.verifyPong(parsed, key))
    }

    @Test fun tamperedTagRejected() {
        val key = ByteArray(32) { 6.toByte() }
        val session = ByteArray(16) { 3.toByte() }
        val nonce = ByteArray(16) { 4.toByte() }
        val pkt = PonyDirectPunch.probe(key, session, nonce)
        pkt[pkt.size - 1] = (pkt[pkt.size - 1].toInt() xor 0x01).toByte()
        val parsed = PonyDirectPunch.parse(pkt)!!
        assertFalse(PonyDirectPunch.verifyProbe(parsed, key))
    }

    @Test fun keepaliveShape() {
        val session = ByteArray(16) { 7.toByte() }
        val ka = PonyDirectPunch.keepalive(session)
        assertEquals(PonyDirectPunch.KEEPALIVE_LEN, ka.size)
        val parsed = PonyDirectPunch.parse(ka)!!
        assertEquals(PonyDirectPunch.KEEPALIVE, parsed.type)
        assertTrue(session.contentEquals(parsed.sessionNonce))
    }

    @Test fun wrongLengthRejected() {
        assertNull(PonyDirectPunch.parse(byteArrayOf(0x11, 0x00, 0x00)))
    }
}
