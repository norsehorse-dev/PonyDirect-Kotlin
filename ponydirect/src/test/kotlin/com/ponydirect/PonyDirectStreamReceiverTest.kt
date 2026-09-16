package com.ponydirect

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PonyDirectStreamReceiverTest {

    private val stream = ByteArray(4 * 1024 + 500) { (it % 251).toByte() }

    private fun feed(r: PonyDirectStreamReceiver, i: Int) {
        val start = i * 1024
        val end = minOf(start + 1024, stream.size)
        r.onData(start.toLong(), stream.copyOfRange(start, end))
    }

    @Test fun reassemblesOutOfOrderWithGapDupAndFin() {
        val r = PonyDirectStreamReceiver()
        feed(r, 2)
        assertEquals(0L, r.cumAck())
        assertEquals(listOf(PonyDirectStream.SackBlock(2048L, 3072L)), r.sackBlocks())

        feed(r, 0); feed(r, 1); feed(r, 1); feed(r, 4); feed(r, 3)
        assertEquals(stream.size.toLong(), r.cumAck())
        assertEquals(emptyList<PonyDirectStream.SackBlock>(), r.sackBlocks())

        r.onFin(stream.size.toLong())
        assertTrue(r.isComplete())
        assertArrayEquals(stream, r.read(stream.size))
    }

    @Test fun twoDisjointGapsGiveTwoSackBlocks() {
        val r = PonyDirectStreamReceiver()
        feed(r, 1); feed(r, 3)   // chunk 0 and 2 still missing
        assertEquals(0L, r.cumAck())
        assertEquals(
            listOf(PonyDirectStream.SackBlock(1024L, 2048L), PonyDirectStream.SackBlock(3072L, 4096L)),
            r.sackBlocks(),
        )
    }

    @Test fun rwndShrinksWhileBufferingOutOfOrder() {
        val r = PonyDirectStreamReceiver(capacity = 8 * 1024)
        val full = r.rwnd()
        feed(r, 1)   // buffered out of order, not yet deliverable
        assertTrue(r.rwnd() < full)
        assertEquals(full - 1024, r.rwnd())
    }
}
