package com.ponydirect

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Drives a real PonyDirectStreamSender and PonyDirectStreamReceiver against a virtual-clock channel
 * with deterministic loss, reordering (latency jitter) and delay, and asserts the whole transfer
 * arrives byte-identical. This is what validates the ARQ end to end under adversarial conditions.
 */
class PonyDirectStreamLoopbackTest {

    // Deterministic LCG so failures reproduce.
    private class Rng(private var s: Long) {
        fun next(): Int { s = s * 6364136223846793005L + 1442695040888963407L; return ((s ushr 33).toInt()) and 0x7fffffff }
        fun dropped(pct: Int): Boolean = next() % 100 < pct
        fun jitter(n: Int): Long = if (n <= 0) 0L else (next() % n).toLong()
    }

    private class Item(
        val at: Long, val kind: Int, val seq: Long, val payload: ByteArray,
        val cumAck: Long, val rwnd: Int, val blocks: List<PonyDirectStream.SackBlock>,
    )

    private fun run(totalBytes: Int, dropFwdPct: Int, dropRevPct: Int, seed: Long) {
        val original = ByteArray(totalBytes) { ((it * 31 + 7) % 251).toByte() }
        val s = PonyDirectStreamSender()
        val r = PonyDirectStreamReceiver()
        s.write(original); s.finish()
        val rng = Rng(seed)
        val baseLat = 30L; val jitter = 40

        val fwd = ArrayList<Item>()
        val rev = ArrayList<Item>()
        var now = 0L
        val step = 20L
        var steps = 0
        while (!r.isComplete() && steps < 400_000) {
            for (o in s.poll(now)) when (o) {
                is StreamOut.Data -> if (!rng.dropped(dropFwdPct))
                    fwd.add(Item(now + baseLat + rng.jitter(jitter), 0, o.seq, o.payload, 0, 0, emptyList()))
                is StreamOut.Fin -> if (!rng.dropped(dropFwdPct))
                    fwd.add(Item(now + baseLat + rng.jitter(jitter), 1, o.finalSeq, ByteArray(0), 0, 0, emptyList()))
            }
            val fdue = ArrayList<Item>(); val fkeep = ArrayList<Item>()
            for (it in fwd) (if (it.at <= now) fdue else fkeep).add(it)
            fwd.clear(); fwd.addAll(fkeep)
            for (it in fdue) if (it.kind == 0) r.onData(it.seq, it.payload) else r.onFin(it.seq)

            if (!rng.dropped(dropRevPct))
                rev.add(Item(now + baseLat + rng.jitter(jitter), 2, 0, ByteArray(0), r.cumAck(), r.rwnd(), r.sackBlocks()))
            val rdue = ArrayList<Item>(); val rkeep = ArrayList<Item>()
            for (it in rev) (if (it.at <= now) rdue else rkeep).add(it)
            rev.clear(); rev.addAll(rkeep)
            for (it in rdue) s.onAck(now, it.cumAck, it.rwnd, it.blocks)

            now += step; steps++
        }
        assertTrue("transfer did not complete within the step budget", r.isComplete())
        assertArrayEquals(original, r.read(totalBytes))
        assertTrue(s.isDone())
    }

    @Test fun cleanChannel() = run(100 * 1024 + 137, 0, 0, 42L)
    @Test fun lossAndReorder() = run(200 * 1024 + 500, 20, 10, 0x12345678L)
    @Test fun heavyLoss() = run(64 * 1024 + 33, 40, 30, 0xC0FFEEL)
}
