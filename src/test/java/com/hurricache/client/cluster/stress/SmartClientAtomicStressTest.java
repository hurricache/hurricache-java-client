package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.AtomicCasRes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for atomic operations (exchange, add, sub, and, or, xor, compareAndSet).
 * 
 * Configurable parameters (static final):
 * - WARMUP_ITERATIONS: warmup phase iterations (default: 10000)
 * - BENCHMARK_ITERATIONS: benchmark phase iterations (default: 100000)
 * - CONCURRENCY: concurrent in-flight requests (default: 100)
 * - THREAD_COUNT: number of threads (default: 4)
 */
public class SmartClientAtomicStressTest extends TestBaseCluster {
    
    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 10000;
    private static final int BENCHMARK_ITERATIONS = 100000;
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
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.exchange(key, hint, i).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Number> future = client.exchange(key, hint, i);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Exchange(long)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: exchange (double)")
    void stressTestExchangeDouble() throws Exception {
        String key = "stress_exchange_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.exchange(key, hint, i * 1.0).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Number> future = client.exchange(key, hint, i * 1.0);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
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
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.add(key, hint, 1).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Number> future = client.add(key, hint, 1);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Add(long)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: add (double)")
    void stressTestAddDouble() throws Exception {
        String key = "stress_add_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.add(key, hint, 1.0).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Number> future = client.add(key, hint, 1.0);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
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
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.sub(key, hint, 1).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Number> future = client.sub(key, hint, 1);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Sub(long)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: sub (double)")
    void stressTestSubDouble() throws Exception {
        String key = "stress_sub_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(BENCHMARK_ITERATIONS * 1.0)).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.sub(key, hint, 1.0).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Number> future = client.sub(key, hint, 1.0);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
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
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.and(key, hint, 0xFF).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Long> future = client.and(key, hint, 0xFF);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("And", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: or")
    void stressTestOr() throws Exception {
        String key = "stress_or_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.or(key, hint, 0xFF).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Long> future = client.or(key, hint, 0xFF);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Or", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: xor")
    void stressTestXor() throws Exception {
        String key = "stress_xor_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.xor(key, hint, 0xFF).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Long> future = client.xor(key, hint, 0xFF);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
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
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.compareAndSet(key, hint, i % 2, i % 2 + 1).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<AtomicCasRes> future = client.compareAndSet(key, hint, i % 2, i % 2 + 1);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("CAS(long)", iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: compareAndSet (double)")
    void stressTestCasDouble() throws Exception {
        String key = "stress_cas_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.compareAndSet(key, hint, i % 2.0, i % 2.0 + 1.0).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<AtomicCasRes> future = client.compareAndSet(key, hint, i % 2.0, i % 2.0 + 1.0);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
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
        
        // Wait for replication
        Thread.sleep(500);
        
        int iterationsPerThread = BENCHMARK_ITERATIONS / THREAD_COUNT;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong totalErrors = new AtomicLong(0);
        
        List<Thread> threads = new ArrayList<>();
        long startTime = System.nanoTime();
        
        for (int t = 0; t < THREAD_COUNT; t++) {
            Thread bt = new Thread(() -> {
                try { startLatch.await(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                
                for (int i = 0; i < iterationsPerThread; i++) {
                    CompletableFuture<Number> future = client.add(key, hint, 1);
                    future.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                }
                doneLatch.countDown();
            });
            threads.add(bt);
            bt.start();
        }
        
        startLatch.countDown();
        doneLatch.await();
        Thread.sleep(500);
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