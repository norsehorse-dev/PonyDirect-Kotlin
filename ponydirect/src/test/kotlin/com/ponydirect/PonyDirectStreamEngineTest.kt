package com.ponydirect

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * End-to-end over a simulated wire: two engines sharing a per-pair key and session nonce move a
 * payload through real SDATA/SACK/SFIN frames across a lossy, reordering channel. Exercises the full
 * stack (codec, tags, ARQ, congestion control) the way PonyDirectWan will run it.
 */
class PonyDirectStreamEngineTest {

    private class Rng(private var s: Long) {
        fun next(): Int { s = s * 6364136223846793005L + 1442695040888963407L; return ((s ushr 33).toInt()) and 0x7fffffff }
        fun dropped(pct: Int) = next() % 100 < pct
        fun jitter(n: Int) = if (n <= 0) 0L else (next() % n).toLong()
    }

    private class Item(val at: Long, val d: ByteArray)

    private fun run(totalBytes: Int, dropPct: Int, seed: Long) {
        val key = ByteArray(32) { (it * 7 + 1).toByte() }
        val nonce = ByteArray(16) { (it + 3).toByte() }
        val original = ByteArray(totalBytes) { ((it * 31 + 7) % 251).toByte() }
        val rng = Rng(seed)
        val baseLat = 30L; val jit = 40
        val aToB = ArrayList<Item>(); val bToA = ArrayList<Item>()
        var now = 0L

        val a = PonyDirectStreamEngine(key, nonce) { d -> if (!rng.dropped(dropPct)) aToB.add(Item(now + baseLat + rng.jitter(jit), d)) }
        val b = PonyDirectStreamEngine(key, nonce) { d -> if (!rng.dropped(dropPct)) bToA.add(Item(now + baseLat + rng.jitter(jit), d)) }
        a.write(original); a.finishSending()

        var steps = 0
        while (!b.recvComplete() && steps < 400_000) {
            a.tick(now); b.tick(now)
            val ad = ArrayList<Item>(); val ak = ArrayList<Item>()
            for (it in aToB) (if (it.at <= now) ad else ak).add(it); aToB.clear(); aToB.addAll(ak)
            for (it in ad) b.onWireDatagram(now, it.d)
            val bd = ArrayList<Item>(); val bk = ArrayList<Item>()
            for (it in bToA) (if (it.at <= now) bd else bk).add(it); bToA.clear(); bToA.addAll(bk)
            for (it in bd) a.onWireDatagram(now, it.d)
            now += 20; steps++
        }
        assertTrue("transfer did not complete within the step budget", b.recvComplete())
        assertArrayEquals(original, b.read(totalBytes))
    }

    @Test fun cleanWire() = run(80 * 1024 + 21, 0, 7L)
    @Test fun lossyReorderingWire() = run(150 * 1024 + 99, 20, 0x51EED10DL)
}
