package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

/**
 * Common base class for all cluster stress tests.
 * Provides unified, DRY execution engines for single-threaded and multi-threaded benchmarks
 * with per-operation latency measurement (buckets p50/p95/p99/max) and real error breakdown.
 */
public abstract class BaseClusterStressTest extends TestBaseCluster {

    protected static final int WARMUP_ITERATIONS = 10000;
    protected static final int BENCHMARK_ITERATIONS = 100000;
    protected static final int CONCURRENCY = 100;
    protected static final int THREAD_COUNT = 4;
    protected static final int CONTAINER_SIZE = 1000;

    protected static final int KEYSPACE_SIZE = 10000;
    protected static final int VALUE_SIZE_BYTES = 512;

    // Latency buckets (microseconds, upper bound; last = overflow)
    protected static final long[] BUCKET_BOUNDS_US =
            {50, 100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600, 51_200, 102_400};

    protected static ExecutorService threadPool;

    @BeforeAll
    static void initThreadPool() {
        threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterAll
    static void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {
        R apply(T t) throws Exception;
    }

    // =========================================================================
    // KEYSPACE — create and WAIT for every create to be confirmed
    // =========================================================================

    protected static class Keyspace {
        public final String[] keys;
        public final KeyHintData[] hints;

        Keyspace(int size) {
            keys = new String[size];
            hints = new KeyHintData[size];
        }

        public int randomIndex() {
            return ThreadLocalRandom.current().nextInt(keys.length);
        }
    }

    protected Keyspace createKeyspace(String prefix, IntFunction<Variant> valueFactory) throws Exception {
        return createKeyspace(prefix, KEYSPACE_SIZE, valueFactory);
    }

    protected Keyspace createKeyspace(String prefix, int size, IntFunction<Variant> valueFactory) throws Exception {
        Keyspace ks = new Keyspace(size);
        AtomicLong createErrors = new AtomicLong(0);
        List<CompletableFuture<KeyHintData>> pending = new ArrayList<>(size);

        long start = System.nanoTime();
        for (int i = 0; i < size; i++) {
            ks.keys[i] = prefix + i;
            final int idx = i;
            CompletableFuture<KeyHintData> f = client.create(ks.keys[i], valueFactory.apply(i));
            pending.add(f.whenComplete((hint, ex) -> {
                if (ex == null) {
                    ks.hints[idx] = hint;
                } else {
                    createErrors.incrementAndGet();
                }
            }));
        }
        // Ждём подтверждения КАЖДОГО create — иначе зависимые операции дадут лавину ложных NOT_FOUND
        CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
        double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;

        long missing = 0;
        for (int i = 0; i < size; i++) {
            if (ks.hints[i] == null) missing++;
        }
        System.out.printf("[Keyspace %s] created=%d/%d, createErrors=%d, %.3fs, %.2f OPS%n",
                prefix, size - missing, size, createErrors.get(), elapsed, size / elapsed);

        if (missing > 0) {
            throw new IllegalStateException("Keyspace incomplete: " + missing + " keys not created");
        }
        // Массовое создание: ждём доставки репликации на backup по выборочным ключам
        String first = ks.keys[0];
        String middle = ks.keys[size / 2];
        String last = ks.keys[size - 1];
        waitForCondition(() ->
                client.setMode(Mode.BACKUP).containsKey(first).get()
                        && client.setMode(Mode.MASTER).containsKey(first).get()
                        && client.setMode(Mode.BACKUP).containsKey(middle).get()
                        && client.setMode(Mode.MASTER).containsKey(middle).get()
                        && client.setMode(Mode.BACKUP).containsKey(last).get()
                        && client.setMode(Mode.MASTER).containsKey(last).get());
        return ks;
    }

    protected Keyspace createBytesKeyspace(String prefix) throws Exception {
        byte[] payload = new byte[VALUE_SIZE_BYTES];
        ThreadLocalRandom.current().nextBytes(payload);
        String value = new String(payload);
        return createKeyspace(prefix, i -> v(value));
    }

    /**
     * Keyspace с клиентскими хинтами (client-managed hashes): каждый ключ создаётся
     * с явно заданным KeyHintData, подтверждение — все future'ы + репликация на backup.
     */
    protected Keyspace createKeyspaceWithHints(String prefix, int size, IntFunction<Variant> valueFactory,
                                               IntFunction<KeyHintData> hintFactory) throws Exception {
        Keyspace ks = new Keyspace(size);
        AtomicLong createErrors = new AtomicLong(0);
        List<CompletableFuture<KeyHintData>> pending = new ArrayList<>(size);

        long start = System.nanoTime();
        for (int i = 0; i < size; i++) {
            ks.keys[i] = prefix + i;
            ks.hints[i] = hintFactory.apply(i);
            final int idx = i;
            CompletableFuture<KeyHintData> f = client.create(ks.keys[i], ks.hints[i], valueFactory.apply(i));
            pending.add(f.whenComplete((hint, ex) -> {
                if (ex != null) createErrors.incrementAndGet();
            }));
        }
        CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
        double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("[Keyspace(custom hints) %s] created=%d/%d, createErrors=%d, %.3fs, %.2f OPS%n",
                prefix, size - createErrors.get(), size, createErrors.get(), elapsed, size / elapsed);

        if (createErrors.get() > 0) {
            throw new IllegalStateException("Keyspace incomplete: " + createErrors.get() + " keys not created");
        }
        // Ждём репликацию на backup по выборочным ключам
        String first = ks.keys[0];
        String middle = ks.keys[size / 2];
        String last = ks.keys[size - 1];
        waitForCondition(() ->
                client.setMode(Mode.BACKUP).containsKey(first).get()
                        && client.setMode(Mode.MASTER).containsKey(first).get()
                        && client.setMode(Mode.BACKUP).containsKey(middle).get()
                        && client.setMode(Mode.MASTER).containsKey(middle).get()
                        && client.setMode(Mode.BACKUP).containsKey(last).get()
                        && client.setMode(Mode.MASTER).containsKey(last).get());
        return ks;
    }

    // =========================================================================
    // METRICS
    // =========================================================================

    protected static class Metrics {
        final AtomicLong ops = new AtomicLong(0);
        final AtomicLong errors = new AtomicLong(0);
        final AtomicLong latencySumNanos = new AtomicLong(0);
        final AtomicLong latencyMaxNanos = new AtomicLong(0);
        final AtomicLongArray buckets = new AtomicLongArray(BUCKET_BOUNDS_US.length + 1);
        final Map<String, AtomicLong> errorsByType = new ConcurrentHashMap<>();

        void record(long latencyNanos, Throwable ex) {
            if (ex != null) {
                errors.incrementAndGet();
                String type = ex.getClass().getSimpleName();
                String msg = ex.getMessage();
                if (msg != null && msg.length() > 60) msg = msg.substring(0, 60);
                errorsByType.computeIfAbsent(type + (msg != null ? ": " + msg : ""), k -> new AtomicLong(0))
                        .incrementAndGet();
                return;
            }
            ops.incrementAndGet();
            latencySumNanos.addAndGet(latencyNanos);
            latencyMaxNanos.accumulateAndGet(latencyNanos, Math::max);
            long us = latencyNanos / 1_000;
            int b = 0;
            while (b < BUCKET_BOUNDS_US.length && us > BUCKET_BOUNDS_US[b]) b++;
            buckets.incrementAndGet(b);
        }

        long percentile(double p) {
            long total = ops.get();
            if (total == 0) return 0;
            long threshold = (long) Math.ceil(total * p);
            long cumulative = 0;
            for (int b = 0; b < buckets.length(); b++) {
                cumulative += buckets.get(b);
                if (cumulative >= threshold) {
                    return b < BUCKET_BOUNDS_US.length ? BUCKET_BOUNDS_US[b] : -1;
                }
            }
            return -1;
        }
    }

    protected void runMetricsBenchmark(String name, int iterations,
                                       ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        runMetricsBenchmark(name, -1, iterations, action);
    }

    protected void runMetricsBenchmark(String name, int containerSize, int iterations,
                                       ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        Metrics m = new Metrics();
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            long opStart = System.nanoTime();
            CompletableFuture<?> f;
            try {
                f = action.apply(i);
            } catch (Exception e) {
                semaphore.release();
                m.record(System.nanoTime() - opStart, e);
                continue;
            }
            if (f != null) {
                futures.add(f.whenComplete((res, ex) -> {
                    semaphore.release();
                    m.record(System.nanoTime() - opStart, ex);
                }));
            } else {
                semaphore.release();
                m.record(System.nanoTime() - opStart, null);
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;

        printMetrics(name, containerSize, iterations, elapsed, m);
    }

    protected void runMetricsBenchmarkMT(String name, int totalIterations, int threads,
                                         BiFunction<Integer, Integer, CompletableFuture<?>> action) throws Exception {
        runMetricsBenchmarkMT(name, -1, totalIterations, threads, action);
    }

    protected void runMetricsBenchmarkMT(String name, int containerSize, int totalIterations, int threads,
                                         BiFunction<Integer, Integer, CompletableFuture<?>> action) throws Exception {
        Metrics m = new Metrics();
        int perThread = totalIterations / threads;
        CountDownLatch latch = new CountDownLatch(threads);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>(totalIterations));

        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            threadPool.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        semaphore.acquire();
                        long opStart = System.nanoTime();
                        CompletableFuture<?> f;
                        try {
                            f = action.apply(threadId, i);
                        } catch (Exception e) {
                            semaphore.release();
                            m.record(System.nanoTime() - opStart, e);
                            continue;
                        }
                        if (f != null) {
                            futures.add(f.whenComplete((res, ex) -> {
                                semaphore.release();
                                m.record(System.nanoTime() - opStart, ex);
                            }));
                        } else {
                            semaphore.release();
                            m.record(System.nanoTime() - opStart, null);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;

        printMetrics(name + " MT", containerSize, totalIterations, elapsed, m);
    }

    protected void printMetrics(String name, int containerSize, int iterations, double elapsedSec, Metrics m) {
        long ops = m.ops.get();
        double avgUs = ops > 0 ? m.latencySumNanos.get() / 1_000.0 / ops : 0;
        String sizePart = containerSize > 0 ? "size=" + containerSize + ", " : "";
        System.out.printf(
                "[%s] %stotal=%d, ok=%d, errors=%d, %.3fs, %.2f OPS | latency us: avg=%.1f, p50=%s, p95=%s, p99=%s, max=%d%n",
                name, sizePart, iterations, ops, m.errors.get(), elapsedSec, iterations / elapsedSec,
                avgUs, fmtBound(m.percentile(0.50)), fmtBound(m.percentile(0.95)),
                fmtBound(m.percentile(0.99)), m.latencyMaxNanos.get() / 1_000);

        if (!m.errorsByType.isEmpty()) {
            System.out.printf("[%s] error breakdown:%n", name);
            m.errorsByType.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                    .limit(5)
                    .forEach(e -> System.out.printf("    %8d  %s%n", e.getValue().get(), e.getKey()));
        }
    }

    protected static String fmtBound(long us) {
        return us < 0 ? ">" + BUCKET_BOUNDS_US[BUCKET_BOUNDS_US.length - 1] : Long.toString(us);
    }

    // =========================================================================
    // LEGACY ENGINES (без метрик) — оставлены для постепенной миграции
    // =========================================================================

    protected void runWarmup(int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<?> f = action.apply(i);
            if (f != null) {
                f.get();
            }
        }
    }

    protected void runBenchmark(String name, int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        runBenchmark(name, -1, iterations, action);
    }

    protected void runBenchmark(String name, int containerSize, int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<?> f;
            try {
                f = action.apply(i);
            } catch (Exception e) {
                semaphore.release();
                errors.incrementAndGet();
                continue;
            }

            if (f != null) {
                futures.add(f.whenComplete((res, ex) -> {
                    semaphore.release();
                    if (ex != null) errors.incrementAndGet();
                }));
            } else {
                semaphore.release();
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        printResults(name, containerSize, iterations, System.nanoTime() - start, errors.get());
    }

    protected void runMultiThreadedBenchmark(String name, int totalIterations, int threads, BiFunction<Integer, Integer, CompletableFuture<?>> action) throws Exception {
        runMultiThreadedBenchmark(name, -1, totalIterations, threads, action);
    }

    protected void runMultiThreadedBenchmark(String name, int containerSize, int totalIterations, int threads, BiFunction<Integer, Integer, CompletableFuture<?>> action) throws Exception {
        int perThread = totalIterations / threads;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>(totalIterations));

        long start = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            threadPool.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        semaphore.acquire();
                        CompletableFuture<?> f;
                        try {
                            f = action.apply(threadId, i);
                        } catch (Exception e) {
                            semaphore.release();
                            errors.incrementAndGet();
                            continue;
                        }

                        if (f != null) {
                            futures.add(f.whenComplete((res, ex) -> {
                                semaphore.release();
                                if (ex != null) errors.incrementAndGet();
                            }));
                        } else {
                            semaphore.release();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        printResults(name + " MT", containerSize, totalIterations, System.nanoTime() - start, errors.get());
    }

    // Helpers for test data generation
    protected Map<Payload, Variant> generateMapData(int size) {
        Map<Payload, Variant> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(p("key_" + i), v("value_" + i));
        }
        return map;
    }

    protected List<Variant> generateListData(int size) {
        List<Variant> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(v("item_" + (i % 100)));
        }
        return list;
    }

    protected void printResults(String operation, int containerSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        if (containerSize > 0) {
            System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n", operation, containerSize, iterations, elapsed, ops, errors);
        } else {
            System.out.printf("[%s] %d iters, %.3fs, %.2f OPS, %d errors%n", operation, iterations, elapsed, ops, errors);
        }
    }
}
