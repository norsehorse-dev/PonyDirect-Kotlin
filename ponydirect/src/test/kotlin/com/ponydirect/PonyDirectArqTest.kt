package com.ponydirect

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PonyDirectArqTest {

    @Test fun dataRoundTripAndAuth() {
        val key = ByteArray(32) { 7.toByte() }
        val session = ByteArray(16) { 1.toByte() }
        val payload = ByteArray(200) { (it % 256).toByte() }
        val pkt = PonyDirectArq.data(key, session, 0x01020304, 5, 10, payload)
        val p = PonyDirectArq.parseData(pkt); assertNotNull(p)
        assertEquals(0x01020304, p!!.msgSeq)
        assertEquals(5, p.chunkIndex)
        assertEquals(10, p.chunkCount)
        assertTrue(payload.contentEquals(p.payload))
        assertTrue(PonyDirectArq.verifyData(p, key))
        assertFalse(PonyDirectArq.verifyData(p, ByteArray(32) { 9.toByte() }))
    }

    @Test fun dataTamperRejected() {
        val key = ByteArray(32) { 7.toByte() }
        val session = ByteArray(16) { 1.toByte() }
        val pkt = PonyDirectArq.data(key, session, 1, 0, 1, byteArrayOf(1, 2, 3))
        pkt[26] = (pkt[26].toInt() xor 0x01).toByte()
        val p = PonyDirectArq.parseData(pkt)!!
        assertFalse(PonyDirectArq.verifyData(p, key))
    }

    @Test fun emptyPayloadChunk() {
        val key = ByteArray(32) { 2.toByte() }
        val session = ByteArray(16) { 3.toByte() }
        val pkt = PonyDirectArq.data(key, session, 1, 0, 1, ByteArray(0))
        assertEquals(57, pkt.size)
        val p = PonyDirectArq.parseData(pkt)!!
        assertEquals(0, p.payload.size)
        assertTrue(PonyDirectArq.verifyData(p, key))
    }

    @Test fun ackRoundTripAndAuth() {
        val key = ByteArray(32) { 4.toByte() }
        val session = ByteArray(16) { 5.toByte() }
        val bitmap = ByteArray(PonyDirectArq.bitmapLen(10))
        PonyDirectArq.bitmapSet(bitmap, 0)
        PonyDirectArq.bitmapSet(bitmap, 9)
        val pkt = PonyDirectArq.ack(key, session, 42, 10, bitmap)
        val p = PonyDirectArq.parseAck(pkt)!!
        assertEquals(42, p.msgSeq)
        assertEquals(10, p.chunkCount)
        assertTrue(PonyDirectArq.verifyAck(p, key))
        assertTrue(PonyDirectArq.bitmapGet(p.bitmap, 0))
        assertTrue(PonyDirectArq.bitmapGet(p.bitmap, 9))
        assertFalse(PonyDirectArq.bitmapGet(p.bitmap, 5))
    }

    @Test fun chunking() {
        val payload = ByteArray(2500) { 0xAB.toByte() }
        val chunks = PonyDirectArq.chunk(payload)
        assertEquals(3, chunks.size)
        assertEquals(1024, chunks[0].size)
        assertEquals(1024, chunks[1].size)
        assertEquals(452, chunks[2].size)
        assertEquals(2500, chunks.sumOf { it.size })
    }
}
