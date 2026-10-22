package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
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
 * Stress tests for Set operations (createSet, getSet, addElementUnordered, containsKey, removeFromContainerByKeyAndValue, removeFromContainerByKey, getSize).
 * 
 * Configurable parameters (static final):
 * - WARMUP_ITERATIONS: warmup phase iterations (default: 10000)
 * - BENCHMARK_ITERATIONS: benchmark phase iterations (default: 100000)
 * - CONCURRENCY: concurrent in-flight requests (default: 100)
 * - THREAD_COUNT: number of threads (default: 4)
 * - CONTAINER_SIZE: number of elements in set (default: 1000)
 */
public class SmartClientSetStressTest extends TestBaseCluster {
    
    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 10000;
    private static final int BENCHMARK_ITERATIONS = 100000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 1000;
    
    // =========================================================================
    // SET TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: getSet")
    void stressTestGetSet() throws Exception {
        String key = "stress_s_get_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getSet(key, hint).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<List<Variant>> future = client.getSet(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Set.getSet", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: addElementUnordered")
    void stressTestAddElementUnordered() throws Exception {
        String key = "stress_s_add_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.addElementUnordered(key, hint, List.of(v("new_" + i))).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> future = client.addElementUnordered(key, hint, List.of(v("new_" + i)));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Set.addElementUnordered", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: containsKey")
    void stressTestContainsKey() throws Exception {
        String key = "stress_s_contains_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes()).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Boolean> future = client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes());
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Set.containsKey", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue")
    void stressTestRemoveFromContainerByKeyAndValue() throws Exception {
        String key = "stress_s_remove_val_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE)))).get();
            if (i % 10 == 0) {
                client.addElementUnordered(key, hint, List.of(v("item_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE))));
            delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + i)));
                addFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Set.removeFromContainerByKeyAndValue", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "stress_s_remove_key_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE))).get();
            if (i % 10 == 0) {
                client.addElementUnordered(key, hint, List.of(v("item_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE)));
            delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + i)));
                addFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Set.removeFromContainerByKey", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: getSize")
    void stressTestSetGetSize() throws Exception {
        String key = "stress_s_size_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getSize(key, hint).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> future = client.getSize(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Set.getSize", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    // =========================================================================
    // MULTI-THREADED TEST
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: containsKey (multi-threaded)")
    void stressTestContainsKeyMultiThreaded() throws Exception {
        String key = "stress_s_mt_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
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
                    CompletableFuture<Boolean> future = client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes());
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
        System.out.printf("[Set.containsKey MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    // =========================================================================
    // MULTI-THREADED TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: addElementUnordered (multi-threaded)")
    void stressTestAddElementUnorderedMultiThreaded() throws Exception {
        String key = "stress_s_mt_add_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
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
                    CompletableFuture<Integer> future = client.addElementUnordered(key, hint, List.of(v("new_" + i)));
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
        System.out.printf("[Set.addElementUnordered MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue (multi-threaded)")
    void stressTestRemoveFromContainerByKeyAndValueMultiThreaded() throws Exception {
        String key = "stress_s_mt_remove_val_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
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
                    CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE))));
                    delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + i)));
                        addFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
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
        System.out.printf("[Set.removeFromContainerByKeyAndValue MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKey (multi-threaded)")
    void stressTestRemoveFromContainerByKeyMultiThreaded() throws Exception {
        String key = "stress_s_mt_remove_key_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();
        
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
                    CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE)));
                    delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + i)));
                        addFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
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
        System.out.printf("[Set.removeFromContainerByKey MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
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