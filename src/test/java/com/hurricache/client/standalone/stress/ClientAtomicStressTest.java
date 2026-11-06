package com.hurricache.client.standalone.stress;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.AtomicCasRes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stress tests for atomic operations (exchange, add, sub, and, or, xor, compareAndSet).
 * Fixed version with proper concurrency control (Semaphore) and in-flight request tracking.
 */
public class ClientAtomicStressTest extends TestBase {

    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 5000;
    private static final int BENCHMARK_ITERATIONS = 50000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;

    // =========================================================================
    // EXCHANGE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: exchange (long)")
    void stressTestExchangeLong() throws Exception {
        String key = "stress_exchange_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.exchange(key, hint, i).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final long val = i;
            CompletableFuture<Number> future = client.exchange(key, hint, val);
            CompletableFuture<Number> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Exchange(long)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: exchange (double)")
    void stressTestExchangeDouble() throws Exception {
        String key = "stress_exchange_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.exchange(key, hint, i * 1.0).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final double val = i * 1.0;
            CompletableFuture<Number> future = client.exchange(key, hint, val);
            CompletableFuture<Number> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Exchange(double)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // ADD TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: add (long)")
    void stressTestAddLong() throws Exception {
        String key = "stress_add_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.add(key, hint, 1).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Number> future = client.add(key, hint, 1);
            CompletableFuture<Number> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Add(long)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: add (double)")
    void stressTestAddDouble() throws Exception {
        String key = "stress_add_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.add(key, hint, 1.0).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Number> future = client.add(key, hint, 1.0);
            CompletableFuture<Number> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Add(double)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // SUB TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: sub (long)")
    void stressTestSubLong() throws Exception {
        String key = "stress_sub_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(BENCHMARK_ITERATIONS)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.sub(key, hint, 1).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Number> future = client.sub(key, hint, 1);
            CompletableFuture<Number> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Sub(long)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: sub (double)")
    void stressTestSubDouble() throws Exception {
        String key = "stress_sub_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(BENCHMARK_ITERATIONS * 1.0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.sub(key, hint, 1.0).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Number> future = client.sub(key, hint, 1.0);
            CompletableFuture<Number> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Sub(double)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // BITWISE OPERATIONS TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: and")
    void stressTestAnd() throws Exception {
        String key = "stress_and_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(-1)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.and(key, hint, 0xFF).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Long> future = client.and(key, hint, 0xFF);
            CompletableFuture<Long> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("And", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: or")
    void stressTestOr() throws Exception {
        String key = "stress_or_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.or(key, hint, 0xFF).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Long> future = client.or(key, hint, 0xFF);
            CompletableFuture<Long> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Or", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: xor")
    void stressTestXor() throws Exception {
        String key = "stress_xor_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.xor(key, hint, 0xFF).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Long> future = client.xor(key, hint, 0xFF);
            CompletableFuture<Long> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Xor", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // CAS TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: compareAndSet (long)")
    void stressTestCasLong() throws Exception {
        String key = "stress_cas_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.compareAndSet(key, hint, i % 2, i % 2 + 1).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final long expected = i % 2;
            final long update = i % 2 + 1;
            CompletableFuture<AtomicCasRes> future = client.compareAndSet(key, hint, expected, update);
            CompletableFuture<AtomicCasRes> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("CAS(long)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: compareAndSet (double)")
    void stressTestCasDouble() throws Exception {
        String key = "stress_cas_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.compareAndSet(key, hint, i % 2.0, i % 2.0 + 1.0).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final double expected = i % 2.0;
            final double update = i % 2.0 + 1.0;
            CompletableFuture<AtomicCasRes> future = client.compareAndSet(key, hint, expected, update);
            CompletableFuture<AtomicCasRes> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("CAS(double)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // MULTI-THREADED TEST
    // =========================================================================

    @Test
    @DisplayName("Stress test: add (multi-threaded)")
    void stressTestAddMultiThreaded() throws Exception {
        String key = "stress_add_mt_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();

        int iterationsPerThread = BENCHMARK_ITERATIONS / THREAD_COUNT;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong totalErrors = new AtomicLong(0);

        List<Thread> threads = new ArrayList<>();
        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            Thread bt = new Thread(() -> {
                try {
                    startLatch.await();
                    List<CompletableFuture<?>> threadFutures = new ArrayList<>(iterationsPerThread);
                    Semaphore threadSemaphore = new Semaphore(CONCURRENCY / THREAD_COUNT);

                    for (int i = 0; i < iterationsPerThread; i++) {
                        threadSemaphore.acquire();
                        CompletableFuture<Number> future = client.add(key, hint, 1);
                        CompletableFuture<Number> wrapped = future
                                .exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> threadSemaphore.release());
                        threadFutures.add(wrapped);
                    }
                    CompletableFuture.allOf(threadFutures.toArray(new CompletableFuture[0])).join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            threads.add(bt);
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = BENCHMARK_ITERATIONS / elapsed;
        System.out.printf("[Add MT] %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    private void printResults(String operation, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] %d iters, %.3fs, %.2f OPS, %d errors%n",
                          operation, iterations, elapsed, ops, errors);
    }
}