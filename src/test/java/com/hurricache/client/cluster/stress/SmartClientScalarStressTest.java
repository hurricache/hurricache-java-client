package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for scalar operations (create, get, replace, remove).
 * 
 * Configurable parameters (static final):
 * - WARMUP_ITERATIONS: warmup phase iterations (default: 1000)
 * - BENCHMARK_ITERATIONS: benchmark phase iterations (default: 10000)
 * - THREAD_COUNT: number of threads (1 = single-threaded, default: 1)
 */
public class SmartClientScalarStressTest extends TestBaseCluster {
    
    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 1000;
    private static final int BENCHMARK_ITERATIONS = 10000;
    private static final int THREAD_COUNT = 1;
    
    // =========================================================================
    // SINGLE-THREADED SCALAR TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: create")
    void stressTestCreate() throws Exception {
        int iterations = BENCHMARK_ITERATIONS;
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String key = "stress_create_" + UUID.randomUUID();
            client.create(key, v("value_" + (i % 100))).get();
        }
        long endTime = System.nanoTime();
        printResults("Create", iterations, endTime - startTime, 0);
    }
    
    @Test
    @DisplayName("Stress test: get")
    void stressTestGet() throws Exception {
        // Setup
        String key = "stress_get_" + UUID.randomUUID();
        client.create(key, v("value")).get();
        KeyHintData hint = client.get(key).get().getHint();
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.get(key, hint, getDefaultClientId()).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            client.get(key, hint, getDefaultClientId()).get();
        }
        long endTime = System.nanoTime();
        printResults("Get", iterations, endTime - startTime, 0);
    }
    
    @Test
    @DisplayName("Stress test: replace")
    void stressTestReplace() throws Exception {
        // Setup
        String key = "stress_replace_" + UUID.randomUUID();
        client.create(key, v("old_value")).get();
        KeyHintData hint = client.get(key).get().getHint();
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.replace(key, hint, v("new_value_" + (i % 10)), getDefaultClientId()).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            client.replace(key, hint, v("new_value_" + (i % 100)), getDefaultClientId()).get();
        }
        long endTime = System.nanoTime();
        printResults("Replace", iterations, endTime - startTime, 0);
    }
    
    @Test
    @DisplayName("Stress test: remove")
    void stressTestRemove() throws Exception {
        // Setup
        String key = "stress_remove_" + UUID.randomUUID();
        client.create(key, v("value")).get();
        KeyHintData hint = client.get(key).get().getHint();
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.remove(key, hint, getDefaultClientId()).get();
            client.create(key, v("value")).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            client.remove(key, hint, getDefaultClientId()).get();
            client.create(key, v("value")).get();
        }
        long endTime = System.nanoTime();
        printResults("Remove", iterations, endTime - startTime, 0);
    }
    
    // =========================================================================
    // MULTI-THREADED SCALAR TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: get (multi-threaded)")
    void stressTestGetMultiThreaded() throws Exception {
        if (THREAD_COUNT <= 1) return;
        
        String key = "stress_get_mt_" + UUID.randomUUID();
        client.create(key, v("value")).get();
        KeyHintData hint = client.get(key).get().getHint();
        
        int warmupIters = WARMUP_ITERATIONS / THREAD_COUNT;
        int benchmarkIters = BENCHMARK_ITERATIONS / THREAD_COUNT;
        
        // Warmup
        List<Thread> warmupThreads = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            Thread wt = new Thread(() -> {
                for (int i = 0; i < warmupIters; i++) {
                    try { client.get(key, hint, getDefaultClientId()).get(); }
                    catch (Exception e) { /* ignore */ }
                }
            });
            warmupThreads.add(wt);
            wt.start();
        }
        for (Thread wt : warmupThreads) wt.join();
        
        // Benchmark
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong totalErrors = new AtomicLong(0);
        
        List<Thread> benchmarkThreads = new ArrayList<>();
        long startTime = System.nanoTime();
        
        for (int t = 0; t < THREAD_COUNT; t++) {
            Thread bt = new Thread(() -> {
                try { startLatch.await(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < benchmarkIters; i++) {
                    try { client.get(key, hint, getDefaultClientId()).get(); }
                    catch (Exception e) { totalErrors.incrementAndGet(); }
                }
                doneLatch.countDown();
            });
            benchmarkThreads.add(bt);
            bt.start();
        }
        
        startLatch.countDown();
        doneLatch.await();
        long endTime = System.nanoTime();
        
        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = BENCHMARK_ITERATIONS / elapsed;
        System.out.printf("[Get MT] %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         benchmarkIters, THREAD_COUNT, elapsed, ops, totalErrors.get());
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
