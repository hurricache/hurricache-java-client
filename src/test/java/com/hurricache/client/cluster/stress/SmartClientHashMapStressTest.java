package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for HashMap operations (createMap, getMap, putAll, getInContainer, update, popInContainer, remove, removeFromContainerByKeyAndValue, removeFromContainerByKey, getSize).
 * 
 * Configurable parameters (static final):
 * - WARMUP_ITERATIONS: warmup phase iterations (default: 10000)
 * - BENCHMARK_ITERATIONS: benchmark phase iterations (default: 100000)
 * - CONCURRENCY: concurrent in-flight requests (default: 100)
 * - THREAD_COUNT: number of threads (default: 4)
 * - CONTAINER_SIZE: number of entries in map (default: 1000)
 */
public class SmartClientHashMapStressTest extends TestBaseCluster {
    
    // ====== CONFIGURABLE PARAMETERS ======
    private static final int WARMUP_ITERATIONS = 10000;
    private static final int BENCHMARK_ITERATIONS = 100000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 1000;
    
    // =========================================================================
    // HASHMAP TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: getMap")
    void stressTestGetMap() throws Exception {
        String key = "stress_m_get_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getMap(key, hint).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Map<Payload, Variant>> future = client.getMap(key, hint);
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.getMap", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: putAll")
    void stressTestPutAll() throws Exception {
        String key = "stress_m_put_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> future = client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i)));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.putAll", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: getInContainer")
    void stressTestGetInContainer() throws Exception {
        String key = "stress_m_get_in_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.getInContainer", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: update")
    void stressTestUpdate() throws Exception {
        String key = "stress_m_update_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)).get();
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> future = client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i));
            future.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.update", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: popInContainer")
    void stressTestPopInContainer() throws Exception {
        String key = "stress_m_pop_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        // Wait for replication
        Thread.sleep(5000);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Variant> popFuture = client.popInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            popFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                putFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.popInContainer", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: remove (elementKey)")
    void stressTestRemove() throws Exception {
        String key = "stress_m_remove_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()).get();
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> removeFuture = client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            removeFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                putFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.remove", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue")
    void stressTestRemoveFromContainerByKeyAndValue() throws Exception {
        String key = "stress_m_remove_val_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + (i % 100)));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100)))).get();
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100))));
            delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                putFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.removeFromContainerByKeyAndValue", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "stress_m_remove_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        // Wait for replication
        Thread.sleep(500);
        
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE))).get();
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            }
        }
        
        // Benchmark
        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)));
            delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            if (i % 10 == 0) {
                CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                putFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; });
            }
            
            if (i % CONCURRENCY == 0) {
                Thread.sleep(1);
            }
        }
        Thread.sleep(500);
        long endTime = System.nanoTime();
        printResults("Map.removeFromContainerByKey", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    @Test
    @DisplayName("Stress test: getSize")
    void stressTestMapGetSize() throws Exception {
        String key = "stress_m_size_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
        printResults("Map.getSize", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }
    
    // =========================================================================
    // MULTI-THREADED TEST
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: getInContainer (multi-threaded)")
    void stressTestGetInContainerMultiThreaded() throws Exception {
        String key = "stress_m_mt_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
                    CompletableFuture<Variant> future = client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
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
        System.out.printf("[Map.getInContainer MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    // =========================================================================
    // MULTI-THREADED TESTS
    // =========================================================================
    
    @Test
    @DisplayName("Stress test: putAll (multi-threaded)")
    void stressTestPutAllMultiThreaded() throws Exception {
        String key = "stress_m_mt_put_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
                    CompletableFuture<Integer> future = client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i)));
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
        System.out.printf("[Map.putAll MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: update (multi-threaded)")
    void stressTestUpdateMultiThreaded() throws Exception {
        String key = "stress_m_mt_update_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
                    CompletableFuture<Variant> future = client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i));
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
        System.out.printf("[Map.update MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: popInContainer (multi-threaded)")
    void stressTestPopInContainerMultiThreaded() throws Exception {
        String key = "stress_m_mt_pop_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
                    CompletableFuture<Variant> popFuture = client.popInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
                    popFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                        putFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
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
        System.out.printf("[Map.popInContainer MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: remove (multi-threaded)")
    void stressTestRemoveMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
                    CompletableFuture<Integer> removeFuture = client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
                    removeFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                        putFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
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
        System.out.printf("[Map.remove MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue (multi-threaded)")
    void stressTestRemoveFromContainerByKeyAndValueMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_val_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + (i % 100)));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
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
                    CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100))));
                    delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                        putFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
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
        System.out.printf("[Map.removeFromContainerByKeyAndValue MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                         CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }
    
    @Test
    @DisplayName("Stress test: removeFromContainerByKey (multi-threaded)")
    void stressTestRemoveFromContainerByKeyMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        
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
                    CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)));
                    delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
                    if (i % 10 == 0) {
                        CompletableFuture<Integer> putFuture = client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
                        putFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; });
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
        System.out.printf("[Map.removeFromContainerByKey MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
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