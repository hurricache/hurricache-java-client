package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
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
 * Stress tests for Queue operations (createQueue, pushHead, pushTail, peekHead, peekTail, popHead, popTail, deleteHead, deleteTail, getSize).
 * 
 * Configurable parameters (static final):
 * - WARMUP_ITERATIONS: warmup phase iterations (default: 10000)
 * - BENCHMARK_ITERATIONS: benchmark phase iterations (default: 100000)
 * - CONCURRENCY: concurrent in-flight requests (default: 100)
 * - THREAD_COUNT: number of threads (default: 4)
 * - CONTAINER_SIZE: number of elements in queue (default: 10000)
 */
public class SmartClientQueueStressTest extends TestBaseCluster {
    
    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 10000;
    private static final int BENCHMARK_ITERATIONS = 100000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 10000;
    
    // =========================================================================
    // QUEUE TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: peekHead")
    void stressTestPeekHead() throws Exception {
        // Setup — fill queue with CONTAINER_SIZE elements
        String key = "stress_q_peek_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + (i % 100)));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.peekHead(key, hint).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.peekHead(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.peekHead", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestPeekTail() throws Exception {
        String key = "stress_q_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + (i % 100)));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.peekTail(key, hint).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.peekTail(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.peekTail", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestPushTail() throws Exception {
        String key = "stress_q_push_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.pushTail(key, hint, List.of(v("item_" + i))).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> future = client.pushTail(key, hint, List.of(v("item_" + i)));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.pushTail", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: popHead")
    void stressTestPopHead() throws Exception {
        String key = "stress_q_pop_head_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.popHead(key, hint).get();
            if (i % 10 == 0) {
                client.pushTail(key, hint, List.of(v("item_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> popFuture = client.popHead(key, hint);
            popFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> pushFuture = client.pushTail(key, hint, List.of(v("item_" + i)));
                pushFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.popHead", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: popTail")
    void stressTestPopTail() throws Exception {
        String key = "stress_q_pop_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.popTail(key, hint).get();
            if (i % 10 == 0) {
                client.pushHead(key, hint, List.of(v("item_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> popFuture = client.popTail(key, hint);
            popFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> pushFuture = client.pushHead(key, hint, List.of(v("item_" + i)));
                pushFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.popTail", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: deleteHead")
    void stressTestDeleteHead() throws Exception {
        String key = "stress_q_del_head_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.deleteHead(key, hint).get();
            if (i % 10 == 0) {
                client.pushTail(key, hint, List.of(v("item_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> delFuture = client.deleteHead(key, hint);
            delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> pushFuture = client.pushTail(key, hint, List.of(v("item_" + i)));
                pushFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.deleteHead", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: deleteTail")
    void stressTestDeleteTail() throws Exception {
        String key = "stress_q_del_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.deleteTail(key, hint).get();
            if (i % 10 == 0) {
                client.pushHead(key, hint, List.of(v("item_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> delFuture = client.deleteTail(key, hint);
            delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> pushFuture = client.pushHead(key, hint, List.of(v("item_" + i)));
                pushFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Queue.deleteTail", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    // =========================================================================
    // MULTI-THREADED TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: peekHead (multi-threaded)")
    void stressTestPeekHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Variant> future = client.peekHead(key, hint);
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
        System.out.printf("[Queue.peekHead MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: pushHead (multi-threaded)")
    void stressTestPushHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_push_head_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Integer> future = client.pushHead(key, hint, List.of(v("item_mt_" + i)));
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
        System.out.printf("[Queue.pushHead MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestPushTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_push_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Integer> future = client.pushTail(key, hint, List.of(v("item_mt_" + i)));
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
        System.out.printf("[Queue.pushTail MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestPopHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_pop_head_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Variant> popFuture = client.popHead(key, hint);
                    popFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> pushFuture = client.pushTail(key, hint, List.of(v("item_mt_" + i)));
                        pushFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    }
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
        System.out.printf("[Queue.popHead MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: popTail (multi-threaded)")
    void stressTestPopTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_pop_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Variant> popFuture = client.popTail(key, hint);
                    popFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> pushFuture = client.pushHead(key, hint, List.of(v("item_mt_" + i)));
                        pushFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    }
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
        System.out.printf("[Queue.popTail MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: deleteHead (multi-threaded)")
    void stressTestDeleteHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_del_head_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Integer> delFuture = client.deleteHead(key, hint);
                    delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> pushFuture = client.pushTail(key, hint, List.of(v("item_mt_" + i)));
                        pushFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    }
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
        System.out.printf("[Queue.deleteHead MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: deleteTail (multi-threaded)")
    void stressTestDeleteTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_del_tail_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();
        
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
                    CompletableFuture<Integer> delFuture = client.deleteTail(key, hint);
                    delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> pushFuture = client.pushHead(key, hint, List.of(v("item_mt_" + i)));
                        pushFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    }
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
        System.out.printf("[Queue.deleteTail MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    // =========================================================================
    // HELPERS
    // =========================================================================
    
    private void printResults(String operation, int containerSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n",
                         operation, containerSize, iterations, elapsed, ops, errors);
    }
}