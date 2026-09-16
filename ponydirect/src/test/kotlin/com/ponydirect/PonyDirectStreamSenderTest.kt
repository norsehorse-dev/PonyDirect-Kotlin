package com.ponydirect

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PonyDirectStreamSenderTest {

    private fun seqs(o: List<StreamOut>) = o.filterIsInstance<StreamOut.Data>().map { it.seq }
    private fun hasFin(o: List<StreamOut>, at: Long) = o.any { it is StreamOut.Fin && it.finalSeq == at }

    @Test fun sendsWindowThenFinAfterAck() {
        val s = PonyDirectStreamSender()
        s.write(ByteArray(2500)); s.finish()          // 3 chunks: 1024, 1024, 452
        val f0 = s.poll(0)
        assertEquals(listOf(0L, 1024L, 2048L), seqs(f0))
        assertTrue(f0.none { it is StreamOut.Fin })
        s.onAck(1, 2500L, 1 shl 20, emptyList())
        assertTrue(s.isDone())
        assertTrue(hasFin(s.poll(2), 2500L))
    }

    @Test fun retransmitsUnackedAfterRto() {
        val s = PonyDirectStreamSender(minRtoMs = 1000, initialRtoMs = 1000)
        s.write(ByteArray(2 * 1024)); s.finish()
        assertEquals(listOf(0L, 1024L), seqs(s.poll(0)))
        s.onAck(10, 1024L, 1 shl 20, emptyList())     // only chunk 0 acked; chunk 1 in-flight
        assertTrue(seqs(s.poll(500)).isEmpty())         // before RTO: nothing
        assertEquals(listOf(1024L), seqs(s.poll(1100))) // after RTO: resend chunk 1
        s.onAck(1200, 2048L, 1 shl 20, emptyList())
        assertTrue(s.isDone())
    }

    @Test fun respectsReceiveWindow() {
        val s = PonyDirectStreamSender()
        s.write(ByteArray(10 * 1024)); s.finish()
        s.onAck(0, 0L, 2 * 1024, emptyList())           // receiver advertises room for 2 chunks
        assertEquals(listOf(0L, 1024L), seqs(s.poll(0)))
    }

    @Test fun sackAcksOutOfOrderSoOnlyGapResends() {
        val s = PonyDirectStreamSender(minRtoMs = 1000, initialRtoMs = 1000)
        s.write(ByteArray(3 * 1024)); s.finish()
        assertEquals(listOf(0L, 1024L, 2048L), seqs(s.poll(0)))
        // chunk 0 in order, chunk 2 via SACK; chunk 1 is the gap
        s.onAck(10, 1024L, 1 shl 20, listOf(PonyDirectStream.SackBlock(2048L, 3072L)))
        assertEquals(listOf(1024L), seqs(s.poll(1100)))  // only the gap retransmits
        s.onAck(1200, 3072L, 1 shl 20, emptyList())
        assertTrue(s.isDone())
    }
}
