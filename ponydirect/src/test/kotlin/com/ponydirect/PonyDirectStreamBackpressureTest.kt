package com.ponydirect

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.security.MessageDigest

/**
 * Streaming sends: the sender holds bytes only until they are acknowledged, so a writer that waits
 * on [PonyDirectStreamEngine.sendBufferedBytes] moves a stream far larger than its high-water mark
 * with bounded memory.
 */
class PonyDirectStreamBackpressureTest {

    @Test fun ackedChunksAreReleased() {
        val s = PonyDirectStreamSender()
        s.write(ByteArray(10 * 1024))
        assertEquals(10L * 1024, s.bufferedBytes)
        s.poll(0)
        s.onAck(10, 4L * 1024, 1 shl 20, emptyList())
        assertEquals(6L * 1024, s.bufferedBytes)
        s.write(ByteArray(500)); s.finish()
        assertEquals(6L * 1024 + 500, s.bufferedBytes)
        s.poll(20)
        s.onAck(30, 10L * 1024 + 500, 1 shl 20, emptyList())
        assertEquals(0L, s.bufferedBytes)
        assertTrue(s.isDone())
    }

    @Test fun partialChunksAcrossWritesStayInOrder() {
        val s = PonyDirectStreamSender()
        val r = PonyDirectStreamReceiver()
        val original = ByteArray(5000) { (it % 251).toByte() }
        for (part in listOf(0 to 700, 700 to 2100, 2100 to 2101, 2101 to 5000)) {
            s.write(original.copyOfRange(part.first, part.second))
        }
        s.finish()
        var now = 0L
        while (!s.isDone()) {
            for (o in s.poll(now)) when (o) {
                is StreamOut.Data -> r.onData(o.seq, o.payload)
                is StreamOut.Fin -> r.onFin(o.finalSeq)
            }
            s.onAck(now, r.cumAck(), r.rwnd(), r.sackBlocks())
            now += 20
        }
        assertArrayEquals(original, r.read(10_000))
    }

    @Test fun largeStreamWithHighWaterMark_memoryStaysBounded() {
        val key = ByteArray(32) { (it * 5 + 2).toByte() }
        val nonce = ByteArray(16) { (it + 9).toByte() }
        val total = 24L shl 20                       // 24 MiB through a 1 MiB high-water mark
        val highWater = 1L shl 20
        val piece = 64 * 1024
        val aToB = ArrayList<ByteArray>(); val bToA = ArrayList<ByteArray>()
        val a = PonyDirectStreamEngine(key, nonce) { aToB.add(it) }
        val b = PonyDirectStreamEngine(key, nonce) { bToA.add(it) }
        val sent = MessageDigest.getInstance("SHA-256")
        val got = MessageDigest.getInstance("SHA-256")
        var written = 0L; var received = 0L; var peak = 0L; var now = 0L; var counter = 0
        var steps = 0
        while (!b.recvComplete() && steps < 2_000_000) {
            while (written < total && a.sendBufferedBytes() < highWater) {
                val n = minOf(piece.toLong(), total - written).toInt()
                val chunk = ByteArray(n) { (counter++ % 253).toByte() }
                sent.update(chunk); a.write(chunk); written += n
                if (written == total) a.finishSending()
            }
            peak = maxOf(peak, a.sendBufferedBytes())
            a.tick(now); b.tick(now)
            val toB = ArrayList(aToB); aToB.clear(); toB.forEach { b.onWireDatagram(now, it) }
            val toA = ArrayList(bToA); bToA.clear(); toA.forEach { a.onWireDatagram(now, it) }
            val r = b.read(1 shl 20); got.update(r); received += r.size
            now += 5; steps++
        }
        val tailBytes = b.read(Int.MAX_VALUE); got.update(tailBytes); received += tailBytes.size
        assertTrue("stream did not complete", b.recvComplete())
        assertEquals(total, received)
        assertArrayEquals(sent.digest(), got.digest())
        assertTrue("sender held $peak bytes, more than the high-water mark plus one write", peak <= highWater + piece)
    }
}
