package com.ponydirect

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

// Cross-platform stream-frame vectors, shared with the Swift PonyDirectStreamTests. pairKey =
// bytes 0x00..0x1f, sessionNonce = bytes 0x00..0x0f.
class PonyDirectStreamTest {

    private val key = ByteArray(32) { it.toByte() }
    private val sn = ByteArray(16) { it.toByte() }
    private fun hex(b: ByteArray) = b.joinToString("") { "%02x".format(it.toInt() and 0xFF) }

    @Test fun sdataVectorAndRoundTrip() {
        val f = PonyDirectStream.data(key, sn, 100000L, "hello stream".toByteArray())
        assertEquals("16000102030405060708090a0b0c0d0e0f00000000000186a0000c68656c6c6f2073747265616d81a4d6bb3f3639126352ee4035ae20595e51380b6749cf0e329989583795c35a", hex(f))
        val p = PonyDirectStream.parseData(f)!!
        assertArrayEquals(sn, p.sessionNonce)
        assertEquals(100000L, p.seq)
        assertArrayEquals("hello stream".toByteArray(), p.payload)
        assertTrue(PonyDirectStream.verifyData(p, key))
        assertFalse(PonyDirectStream.verifyData(p, ByteArray(32) { 9.toByte() }))
    }

    @Test fun sackVectorAndRoundTrip() {
        val blocks = listOf(PonyDirectStream.SackBlock(70000L, 71024L), PonyDirectStream.SackBlock(72048L, 73072L))
        val f = PonyDirectStream.ack(key, sn, 65536L, 262144, blocks)
        assertEquals("17000102030405060708090a0b0c0d0e0f000000000001000000040000020000000000011170000000000001157000000000000119700000000000011d70dc1fa9c5c6d5cc419e21e5df35f7003deedf1e37cadd5af6141155760d48a162", hex(f))
        val p = PonyDirectStream.parseAck(f)!!
        assertEquals(65536L, p.cumAck)
        assertEquals(262144, p.rwnd)
        assertEquals(blocks, p.blocks)
        assertTrue(PonyDirectStream.verifyAck(p, key))
    }

    @Test fun sackZeroBlocks() {
        val f = PonyDirectStream.ack(key, sn, 65536L, 262144, emptyList())
        assertEquals("17000102030405060708090a0b0c0d0e0f0000000000010000000400000099217d50aefd2beabe7d29ff5f45c8db5e851737bb7aa739155eab05c4b4ca8a", hex(f))
        assertEquals(emptyList<PonyDirectStream.SackBlock>(), PonyDirectStream.parseAck(f)!!.blocks)
    }

    @Test fun finVectorAndRoundTrip() {
        val f = PonyDirectStream.fin(key, sn, 999999L)
        assertEquals("18000102030405060708090a0b0c0d0e0f00000000000f423f54d7b924e43840818ee903b236e4f9d65413ffbf3c911cbab3263cad78e005e7", hex(f))
        val p = PonyDirectStream.parseFin(f)!!
        assertEquals(999999L, p.finalSeq)
        assertTrue(PonyDirectStream.verifyFin(p, key))
    }

    @Test fun rstVectorAndRoundTrip() {
        val f = PonyDirectStream.rst(key, sn)
        assertEquals("19000102030405060708090a0b0c0d0e0f9e158760dbf00de7046140c7ea88495241193b8dd83805f7f11e61b31fec232a", hex(f))
        val p = PonyDirectStream.parseRst(f)!!
        assertArrayEquals(sn, p.sessionNonce)
        assertTrue(PonyDirectStream.verifyRst(p, key))
    }

    @Test fun largePayloadAndHighSeqRoundTrip() {
        val payload = ByteArray(1024) { (it % 256).toByte() }
        val f = PonyDirectStream.data(key, sn, 0xFFFFFFFF00L, payload)
        val p = PonyDirectStream.parseData(f)!!
        assertEquals(0xFFFFFFFF00L, p.seq)
        assertArrayEquals(payload, p.payload)
        assertTrue(PonyDirectStream.verifyData(p, key))
    }
}
