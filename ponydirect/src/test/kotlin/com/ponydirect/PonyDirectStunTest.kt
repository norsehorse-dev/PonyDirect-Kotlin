package com.ponydirect

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class PonyDirectStunTest {

    @Test fun bindingRequestShape() {
        val txn = ByteArray(12) { it.toByte() }
        val req = PonyDirectStun.bindingRequest(txn)
        assertEquals(20, req.size)
        assertEquals(0x00.toByte(), req[0]); assertEquals(0x01.toByte(), req[1])
        assertEquals(0x00.toByte(), req[2]); assertEquals(0x00.toByte(), req[3])
        assertEquals(0x21.toByte(), req[4]); assertEquals(0x12.toByte(), req[5])
        assertEquals(0xA4.toByte(), req[6]); assertEquals(0x42.toByte(), req[7])
    }

    @Test fun parseXorMappedAddress() {
        val txn = ByteArray(12) { it.toByte() }
        val msg = byteArrayOf(0x01, 0x01, 0x00, 0x0C, 0x21, 0x12, 0xA4.toByte(), 0x42) + txn +
            byteArrayOf(0x00, 0x20, 0x00, 0x08, 0x00, 0x01,
                0xE9.toByte(), 0x30, 0xEA.toByte(), 0x12, 0xD5.toByte(), 0x47)
        val parsed = PonyDirectStun.parseResponse(msg, txn)
        assertEquals("203.0.113.5", parsed?.ip)
        assertEquals(51234, parsed?.port)
    }

    @Test fun wrongTransactionRejected() {
        val txn = ByteArray(12) { it.toByte() }
        val wrong = ByteArray(12) { 0xFF.toByte() }
        val msg = byteArrayOf(0x01, 0x01, 0x00, 0x0C, 0x21, 0x12, 0xA4.toByte(), 0x42) + txn +
            byteArrayOf(0x00, 0x20, 0x00, 0x08, 0x00, 0x01,
                0xE9.toByte(), 0x30, 0xEA.toByte(), 0x12, 0xD5.toByte(), 0x47)
        assertNull(PonyDirectStun.parseResponse(msg, wrong))
    }

    @Test fun plainMappedAddressFallback() {
        val txn = ByteArray(12) { 0xAB.toByte() }
        val msg = byteArrayOf(0x01, 0x01, 0x00, 0x0C, 0x21, 0x12, 0xA4.toByte(), 0x42) + txn +
            byteArrayOf(0x00, 0x01, 0x00, 0x08, 0x00, 0x01,
                0xC8.toByte(), 0x22, 0xCB.toByte(), 0x00, 0x71, 0x05)
        val parsed = PonyDirectStun.parseResponse(msg, txn)
        assertEquals("203.0.113.5", parsed?.ip)
        assertEquals(51234, parsed?.port)
    }
}
