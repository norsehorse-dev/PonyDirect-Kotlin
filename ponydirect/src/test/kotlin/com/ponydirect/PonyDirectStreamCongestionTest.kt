package com.ponydirect

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PonyDirectStreamCongestionTest {

    @Test fun slowStartGrowsThenTimeoutCollapses() {
        val s = PonyDirectStreamSender(minRtoMs = 1000, initialRtoMs = 1000)
        s.write(ByteArray(100 * 1024)); s.finish()
        val init = s.cwndBytes
        s.poll(0)
        s.onAck(50, 4L * 1024, 1 shl 20, emptyList())          // 4 chunks acked in slow start
        assertTrue("slow start should grow cwnd", s.cwndBytes > init)
        val grown = s.cwndBytes
        s.poll(2000)                                           // outstanding chunks time out
        assertTrue("timeout should shrink cwnd", s.cwndBytes < grown)
        assertEquals(1024, s.cwndBytes)                        // timeout restarts slow start at 1 MSS
    }

    @Test fun congestionAvoidanceGrowsSlowerThanSlowStart() {
        val s = PonyDirectStreamSender(
            initialCwndBytes = 4096, initialSsthreshBytes = 4096, minRtoMs = 1000, initialRtoMs = 1000,
        )
        s.write(ByteArray(200 * 1024)); s.finish()
        s.poll(0)
        val before = s.cwndBytes
        s.onAck(50, 4L * 1024, 1 shl 20, emptyList())          // ack 4 chunks while in avoidance
        val delta = s.cwndBytes - before
        assertTrue("avoidance grows by far less than slow start", delta in 1 until 4 * 1024)
    }
}
