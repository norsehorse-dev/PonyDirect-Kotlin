package com.ponydirect

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PonyDirectWireTest {
    @Test fun frameRoundTrip() {
        val payload = "hello world".toByteArray()
        val framed = PonyDirectWire.frame(PonyDirectWire.ENVELOPE, payload)
        val parsed = PonyDirectWire.parseFrame(framed)!!
        assertEquals(PonyDirectWire.ENVELOPE, parsed.type)
        assertArrayEquals(payload, parsed.payload)
        assertEquals(framed.size, parsed.consumed)
    }

    @Test fun partialFrameReturnsNull() {
        val framed = PonyDirectWire.frame(PonyDirectWire.HELLO, byteArrayOf(1, 2, 3, 4))
        assertNull(PonyDirectWire.parseFrame(framed.copyOfRange(0, 6)))
    }

    @Test fun tagsAreDeterministicAndKeyed() {
        val key = ByteArray(32) { 7 }
        val other = ByteArray(32) { 9 }
        val nonce = ByteArray(16) { 1 }
        val a = PonyDirectWire.identifyTag(key, nonce)
        val b = PonyDirectWire.identifyTag(key, nonce)
        val c = PonyDirectWire.identifyTag(other, nonce)
        assertArrayEquals(a, b)
        assertEquals(32, a.size)
        assertFalse(a.contentEquals(c))
        assertTrue(PonyDirectWire.constantTimeEquals(a, b))
        assertFalse(PonyDirectWire.constantTimeEquals(a, c))
    }

    @Test fun ackBindsBothNonces() {
        val key = ByteArray(32) { 3 }
        val dn = ByteArray(16) { 1 }
        val ln = ByteArray(16) { 2 }
        val t1 = PonyDirectWire.identifyAckTag(key, dn, ln)
        val t2 = PonyDirectWire.identifyAckTag(key, ln, dn)
        assertFalse(t1.contentEquals(t2))
    }
}
