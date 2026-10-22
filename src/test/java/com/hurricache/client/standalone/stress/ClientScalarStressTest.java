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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stress tests for scalar operations (create, get, replace, remove) with heterogeneous data types.
 * 
 * Structure:
 * - 1 test per type (bytes, int, double)
 * - 1 heterogeneous test that mixes all types
 * 
 * Configurable parameters (static final):
 * - WARMUP_ITERATIONS: warmup phase iterations (default: 10000)
 * - BENCHMARK_ITERATIONS: benchmark phase iterations (default: 100000)
 * - CONCURRENCY: concurrent in-flight requests (default: 100)
 * - THREAD_COUNT: number of threads (default: 4)
 */
@Execution(ExecutionMode.SAME_THREAD)
public class ClientScalarStressTest extends TestBase {
    
    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 10000;
    private static final int BENCHMARK_ITERATIONS = 100000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    
    // =========================================================================
    // CREATE TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: create (bytes)")
    void stressTestCreateBytes() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String key = "stress_create_bytes_" + UUID.randomUUID();
            CompletableFuture<Boolean> future = client.create(key, v("value_bytes_" + (i % 100))).thenApply(h -> true);
            future.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
        long endTime = System.nanoTime();
        printResults("Create(bytes)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: create (int)")
    void stressTestCreateInt() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String key = "stress_create_int_" + UUID.randomUUID();
            CompletableFuture<Boolean> future = client.create(key, vi(i)).thenApply(h -> true);
            future.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
        long endTime = System.nanoTime();
        printResults("Create(int)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: create (double)")
    void stressTestCreateDouble() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String key = "stress_create_double_" + UUID.randomUUID();
            CompletableFuture<Boolean> future = client.create(key, vd(i * 1.5)).thenApply(h -> true);
            future.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
        long endTime = System.nanoTime();
        printResults("Create(double)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: create (heterogeneous)")
    void stressTestCreateHeterogeneous() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String key = "stress_create_mix_" + UUID.randomUUID();
            Variant value;
            if (i % 3 == 0) {
                value = v("mixed_bytes_" + i);
            } else if (i % 3 == 1) {
                value = vi(i);
            } else {
                value = vd(i * 1.5);
            }
            CompletableFuture<Boolean> future = client.create(key, value).thenApply(h -> true);
            future.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.get(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.get(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.get(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
        long endTime = System.nanoTime();
        printResults("Get(double)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: get (heterogeneous)")
    void stressTestGetHeterogeneous() throws Exception {
        // Create 3 keys with different types
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future;
            if (i % 3 == 0) {
                future = client.get(keyBytes, hintBytes);
            } else if (i % 3 == 1) {
                future = client.get(keyInt, hintInt);
            } else {
                future = client.get(keyDouble, hintDouble);
            }
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.replace(key, hint, v("new_bytes_" + (i % 100)));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.replace(key, hint, vi(i % 100));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.replace(key, hint, vd(i * 1.1));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
        long endTime = System.nanoTime();
        printResults("Replace(double)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: replace (heterogeneous)")
    void stressTestReplaceHeterogeneous() throws Exception {
        // Use 3 separate keys for each type to avoid type mismatch errors
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
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future;
            if (i % 3 == 0) {
                future = client.replace(keyBytes, hintBytes, v("mix_bytes_" + i));
            } else if (i % 3 == 1) {
                future = client.replace(keyInt, hintInt, vi(i % 100));
            } else {
                future = client.replace(keyDouble, hintDouble, vd(i * 1.1));
            }
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });

        }
        long endTime = System.nanoTime();
        printResults("Replace(heterogeneous)", iterations, endTime - startTime, errors.get());
    }
    
    // =========================================================================
    // REMOVE TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: remove (bytes)")
    void stressTestRemoveBytes() throws Exception {
        String key = "stress_remove_bytes_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, v("value_bytes")).get();

        
        // Benchmark - only remove, no re-create (re-create causes ALREADY_EXISTS in cluster)
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Boolean> removeFuture = client.remove(key, hint);
            removeFuture.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
        long endTime = System.nanoTime();
        printResults("Remove(bytes)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: remove (int)")
    void stressTestRemoveInt() throws Exception {
        String key = "stress_remove_int_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(999)).get();

        
        // Benchmark - only remove
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Boolean> removeFuture = client.remove(key, hint);
            removeFuture.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
        long endTime = System.nanoTime();
        printResults("Remove(int)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: remove (double)")
    void stressTestRemoveDouble() throws Exception {
        String key = "stress_remove_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(99.99)).get();

        
        // Benchmark - only remove
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Boolean> removeFuture = client.remove(key, hint);
            removeFuture.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
        long endTime = System.nanoTime();
        printResults("Remove(double)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: remove (heterogeneous)")
    void stressTestRemoveHeterogeneous() throws Exception {
        // Use 3 separate keys for each type
        String keyBytes = "stress_remove_mix_bytes_" + UUID.randomUUID();
        String keyInt = "stress_remove_mix_int_" + UUID.randomUUID();
        String keyDouble = "stress_remove_mix_double_" + UUID.randomUUID();
        
        KeyHintData hintBytes = client.create(keyBytes, v("value_bytes")).get();
        KeyHintData hintInt = client.create(keyInt, vi(999)).get();
        KeyHintData hintDouble = client.create(keyDouble, vd(99.99)).get();

        
        // Benchmark - only remove, no re-create
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Boolean> removeFuture;
            if (i % 3 == 0) {
                removeFuture = client.remove(keyBytes, hintBytes);
            } else if (i % 3 == 1) {
                removeFuture = client.remove(keyInt, hintInt);
            } else {
                removeFuture = client.remove(keyDouble, hintDouble);
            }
            removeFuture.exceptionally(ex -> { errors.incrementAndGet(); return false; });

        }
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
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong totalErrors = new AtomicLong(0);
        
        List<Thread> threads = new ArrayList<>();
        long startTime = System.nanoTime();
        
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                try { startLatch.await(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                
                int seed = threadIdx * 1000;
                for (int i = 0; i < iterationsPerThread; i++) {
                    int idx = (seed + i) % 3;
                    final CompletableFuture<Variant> future;
                    if (idx == 0) future = client.get(keyBytes, hintBytes);
                    else if (idx == 1) future = client.get(keyInt, hintInt);
                    else future = client.get(keyDouble, hintDouble);
                    
                    future.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                }
                doneLatch.countDown();
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