package com.hurricache.client.standalone.stress;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stress tests for scalar operations (create, get, replace, remove) with heterogeneous data types.
 * Fixed version with proper concurrency control and pre-populated keys for remove operations.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class ClientScalarStressTest extends TestBase {

    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 100;
    private static final int BENCHMARK_ITERATIONS = 100000; // Снижено до 50k для стабильности асинхронного пайплайна, можно вернуть 100k
    private static final int CONCURRENCY = 500;
    private static final int THREAD_COUNT = 4;

    // =========================================================================
    // CREATE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: create (bytes)")
    void stressTestCreateBytes() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            String key = "stress_create_bytes_" + i + "_" + UUID.randomUUID();
            CompletableFuture<Boolean> future = client.create(key, v("value_bytes_" + (i % 100)))
                    .thenApply(h -> true);

            CompletableFuture<Boolean> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Create(bytes)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: create (int)")
    void stressTestCreateInt() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            String key = "stress_create_int_" + i + "_" + UUID.randomUUID();
            CompletableFuture<Boolean> future = client.create(key, vi(i))
                    .thenApply(h -> true);

            CompletableFuture<Boolean> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Create(int)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: create (double)")
    void stressTestCreateDouble() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            String key = "stress_create_double_" + i + "_" + UUID.randomUUID();
            CompletableFuture<Boolean> future = client.create(key, vd(i * 1.5))
                    .thenApply(h -> true);

            CompletableFuture<Boolean> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Create(double)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: create (heterogeneous)")
    void stressTestCreateHeterogeneous() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            String key = "stress_create_mix_" + i + "_" + UUID.randomUUID();
            Variant value;
            if (i % 3 == 0) {
                value = v("mixed_bytes_" + i);
            } else if (i % 3 == 1) {
                value = vi(i);
            } else {
                value = vd(i * 1.5);
            }
            CompletableFuture<Boolean> future = client.create(key, value)
                    .thenApply(h -> true);

            CompletableFuture<Boolean> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Create(heterogeneous)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // GET TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: get (bytes)")
    void stressTestGetBytes() throws Exception {
        String key = "stress_get_bytes_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, v("hello_world_bytes_data")).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.get(key, hint).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.get(key, hint);
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Get(bytes)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: get (int)")
    void stressTestGetInt() throws Exception {
        String key = "stress_get_int_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(42)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.get(key, hint).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.get(key, hint);
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Get(int)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: get (double)")
    void stressTestGetDouble() throws Exception {
        String key = "stress_get_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(3.14159265)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.get(key, hint).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.get(key, hint);
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Get(double)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: get (heterogeneous)")
    void stressTestGetHeterogeneous() throws Exception {
        String keyBytes = "stress_get_mix_bytes_" + UUID.randomUUID();
        String keyInt = "stress_get_mix_int_" + UUID.randomUUID();
        String keyDouble = "stress_get_mix_double_" + UUID.randomUUID();

        KeyHintData hintBytes = client.create(keyBytes, v("mixed_bytes_data")).get();
        KeyHintData hintInt = client.create(keyInt, vi(123)).get();
        KeyHintData hintDouble = client.create(keyDouble, vd(2.71828)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            if (i % 3 == 0) client.get(keyBytes, hintBytes).get();
            else if (i % 3 == 1) client.get(keyInt, hintInt).get();
            else client.get(keyDouble, hintDouble).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future;
            if (i % 3 == 0) {
                future = client.get(keyBytes, hintBytes);
            } else if (i % 3 == 1) {
                future = client.get(keyInt, hintInt);
            } else {
                future = client.get(keyDouble, hintDouble);
            }
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Get(heterogeneous)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // REPLACE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: replace (bytes)")
    void stressTestReplaceBytes() throws Exception {
        String key = "stress_replace_bytes_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, v("old_bytes_value")).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.replace(key, hint, v("new_bytes_" + (i % 10))).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.replace(key, hint, v("new_bytes_" + (i % 100)));
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Replace(bytes)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: replace (int)")
    void stressTestReplaceInt() throws Exception {
        String key = "stress_replace_int_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(100)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.replace(key, hint, vi(i % 10)).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.replace(key, hint, vi(i % 100));
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Replace(int)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: replace (double)")
    void stressTestReplaceDouble() throws Exception {
        String key = "stress_replace_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(2.71)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.replace(key, hint, vd(i * 1.1)).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.replace(key, hint, vd(i * 1.1));
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Replace(double)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: replace (heterogeneous)")
    void stressTestReplaceHeterogeneous() throws Exception {
        String keyBytes = "stress_replace_mix_bytes_" + UUID.randomUUID();
        String keyInt = "stress_replace_mix_int_" + UUID.randomUUID();
        String keyDouble = "stress_replace_mix_double_" + UUID.randomUUID();

        KeyHintData hintBytes = client.create(keyBytes, v("initial_bytes")).get();
        KeyHintData hintInt = client.create(keyInt, vi(100)).get();
        KeyHintData hintDouble = client.create(keyDouble, vd(2.71)).get();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            if (i % 3 == 0) client.replace(keyBytes, hintBytes, v("mix_bytes_" + i)).get();
            else if (i % 3 == 1) client.replace(keyInt, hintInt, vi(i % 100)).get();
            else client.replace(keyDouble, hintDouble, vd(i * 1.1)).get();
        }

        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future;
            if (i % 3 == 0) {
                future = client.replace(keyBytes, hintBytes, v("mix_bytes_" + i));
            } else if (i % 3 == 1) {
                future = client.replace(keyInt, hintInt, vi(i % 100));
            } else {
                future = client.replace(keyDouble, hintDouble, vd(i * 1.1));
            }
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Replace(heterogeneous)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // REMOVE TESTS (FIXED: Pre-populate keys to avoid NOT_FOUND errors)
    // =========================================================================

    @Test
    @DisplayName("Stress test: remove (bytes)")
    void stressTestRemoveBytes() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        List<String> keys = new ArrayList<>(iterations);
        List<KeyHintData> hints = new ArrayList<>(iterations);

        // Предварительно создаем ключи для удаления, чтобы замерять именно remove, а не NOT_FOUND
        for (int i = 0; i < iterations; i++) {
            String key = "stress_remove_bytes_" + i + "_" + UUID.randomUUID();
            KeyHintData hint = client.create(key, v("value_bytes")).get();
            keys.add(key);
            hints.add(hint);
        }

        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Boolean> removeFuture = client.remove(keys.get(i), hints.get(i));
            CompletableFuture<Boolean> wrapped = removeFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Remove(bytes)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: remove (int)")
    void stressTestRemoveInt() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        List<String> keys = new ArrayList<>(iterations);
        List<KeyHintData> hints = new ArrayList<>(iterations);

        for (int i = 0; i < iterations; i++) {
            String key = "stress_remove_int_" + i + "_" + UUID.randomUUID();
            KeyHintData hint = client.create(key, vi(999)).get();
            keys.add(key);
            hints.add(hint);
        }

        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Boolean> removeFuture = client.remove(keys.get(i), hints.get(i));
            CompletableFuture<Boolean> wrapped = removeFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Remove(int)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: remove (double)")
    void stressTestRemoveDouble() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        List<String> keys = new ArrayList<>(iterations);
        List<KeyHintData> hints = new ArrayList<>(iterations);

        for (int i = 0; i < iterations; i++) {
            String key = "stress_remove_double_" + i + "_" + UUID.randomUUID();
            KeyHintData hint = client.create(key, vd(99.99)).get();
            keys.add(key);
            hints.add(hint);
        }

        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Boolean> removeFuture = client.remove(keys.get(i), hints.get(i));
            CompletableFuture<Boolean> wrapped = removeFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Remove(double)", iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: remove (heterogeneous)")
    void stressTestRemoveHeterogeneous() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        List<String> keys = new ArrayList<>(iterations);
        List<KeyHintData> hints = new ArrayList<>(iterations);

        for (int i = 0; i < iterations; i++) {
            String key;
            KeyHintData hint;
            if (i % 3 == 0) {
                key = "stress_remove_mix_bytes_" + i + "_" + UUID.randomUUID();
                hint = client.create(key, v("value_bytes")).get();
            } else if (i % 3 == 1) {
                key = "stress_remove_mix_int_" + i + "_" + UUID.randomUUID();
                hint = client.create(key, vi(999)).get();
            } else {
                key = "stress_remove_mix_double_" + i + "_" + UUID.randomUUID();
                hint = client.create(key, vd(99.99)).get();
            }
            keys.add(key);
            hints.add(hint);
        }

        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Boolean> removeFuture = client.remove(keys.get(i), hints.get(i));
            CompletableFuture<Boolean> wrapped = removeFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return false; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();

        printResults("Remove(heterogeneous)", iterations, endTime - startTime, errors.get());
    }

    // =========================================================================
    // MULTI-THREADED TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: get (heterogeneous, multi-threaded)")
    void stressTestGetHeterogeneousMultiThreaded() throws Exception {
        final String keyBytes = "stress_get_mix_mt_bytes_" + UUID.randomUUID();
        final String keyInt = "stress_get_mix_mt_int_" + UUID.randomUUID();
        final String keyDouble = "stress_get_mix_mt_double_" + UUID.randomUUID();

        final KeyHintData hintBytes = client.create(keyBytes, v("mixed_bytes_data")).get();
        final KeyHintData hintInt = client.create(keyInt, vi(123)).get();
        final KeyHintData hintDouble = client.create(keyDouble, vd(2.71828)).get();

        int iterationsPerThread = BENCHMARK_ITERATIONS / THREAD_COUNT;
        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(THREAD_COUNT);
        AtomicLong totalErrors = new AtomicLong(0);

        List<Thread> threads = new ArrayList<>();
        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                try {
                    startLatch.await();
                    List<CompletableFuture<?>> threadFutures = new ArrayList<>(iterationsPerThread);
                    Semaphore threadSemaphore = new Semaphore(CONCURRENCY / THREAD_COUNT);

                    int seed = threadIdx * 1000;
                    for (int i = 0; i < iterationsPerThread; i++) {
                        threadSemaphore.acquire();
                        int idx = (seed + i) % 3;
                        final CompletableFuture<Variant> future;
                        if (idx == 0) future = client.get(keyBytes, hintBytes);
                        else if (idx == 1) future = client.get(keyInt, hintInt);
                        else future = client.get(keyDouble, hintDouble);

                        CompletableFuture<Variant> wrapped = future
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
        System.out.printf("[Get(heterogeneous) MT] %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
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