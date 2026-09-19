// The host's own scaling ceiling, measured before any pipeline is blamed for
// missing it. Two arms: a register-only mix loop with no memory traffic, and a
// random read-modify-write over a 4 MB per-thread array. Written by the agent of
// clean-room run 24, which used it to find what days of one-variable experiments
// here had not: on that host alu work doubled at 98% of linear from 2 to 4 cores
// and mem work at 69%. A pipeline cannot beat its machine.
public class Spin {
  public static void main(String[] a) throws Exception {
    int threads = Integer.parseInt(a[0]);
    long ms = Long.parseLong(a[1]);
    String mode = a.length > 2 ? a[2] : "alu";
    long[] counts = new long[threads];
    Thread[] ts = new Thread[threads];
    long end = System.currentTimeMillis() + ms;
    for (int i = 0; i < threads; i++) {
      final int id = i;
      ts[i] = new Thread(() -> {
        long n = 0; long x = id + 1;
        int[] buf = new int[1 << 20];              // 4 MB per thread
        int idx = 0;
        while (System.currentTimeMillis() < end) {
          for (int k = 0; k < 20000; k++) {
            if (mode.equals("mem")) { idx = (idx * 1664525 + 1013904223) & (buf.length - 1); x += buf[idx]; buf[idx] = (int) x; }
            else { x = x * 6364136223846793005L + 1442695040888963407L; x ^= x >>> 29; }
          }
          n += 20000;
        }
        counts[id] = n + (x & 1) - (x & 1);
      });
      ts[i].start();
    }
    long t0 = System.currentTimeMillis();
    for (Thread t : ts) t.join();
    double s = (System.currentTimeMillis() - t0) / 1000.0;
    long tot = 0; for (long c : counts) tot += c;
    System.out.printf("%s threads=%d %.1fs total=%,d  per-core=%,.0f ops/s%n", mode, threads, s, tot, tot / s / threads);
  }
}
