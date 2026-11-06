package com.hurricache.client.standalone.stress;

import com.hurricache.TestBase;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ClientHashMapStressTest extends TestBase {

    private static final int WARMUP_ITERATIONS = 5000;
    private static final int BENCHMARK_ITERATIONS = 50000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 1000;

    @Test
    @DisplayName("Stress test: getMap")
    void stressTestGetMap() throws Exception {
        String key = "stress_m_get_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + i));
        }
        KeyHintData hint = client.createMap(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getMap(key, hint).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Map<Payload, Variant>> future = client.getMap(key, hint);
            CompletableFuture<Map<Payload, Variant>> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> future = client.putAll(key, hint, List.of(p("new_key_" + idx)), List.of(v("new_value_" + idx)));
            CompletableFuture<Integer> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> future = client.getInContainer(key, hint, ("key_" + (idx % CONTAINER_SIZE)).getBytes());
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> future = client.update(key, hint, ("key_" + (idx % CONTAINER_SIZE)).getBytes(), v("new_value_" + idx));
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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
        Thread.sleep(500);

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> popFuture = client.popInContainer(key, hint, ("key_" + (idx % CONTAINER_SIZE)).getBytes());
            CompletableFuture<Variant> wrapped = popFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.putAll(key, hint, List.of(p("key_" + idx)), List.of(v("value_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> removeFuture = client.remove(key, hint, ("key_" + (idx % CONTAINER_SIZE)).getBytes());
            CompletableFuture<Integer> wrapped = removeFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.putAll(key, hint, List.of(p("key_" + idx)), List.of(v("value_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(
                    key, hint, ContainerType.MAP, List.of(p("key_" + idx)), List.of(v("value_" + (idx % 100))));
            CompletableFuture<Integer> wrapped = delFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.putAll(key, hint, List.of(p("key_" + idx)), List.of(v("value_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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
        Thread.sleep(500);

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, p("key_" + (idx % CONTAINER_SIZE)));
            CompletableFuture<Integer> wrapped = delFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.putAll(key, hint, List.of(p("key_" + idx)), List.of(v("value_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Integer> future = client.getSize(key, hint);
            CompletableFuture<Integer> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Map.getSize", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: getInContainer (multi-threaded)")
    void stressTestGetInContainerMultiThreaded() throws Exception {
        runMultiThreaded("Map.getInContainer MT", () -> {
                             String key = "stress_m_mt_" + UUID.randomUUID();
                             Map<Payload, Variant> initialData = new HashMap<>();
                             for (int i = 0; i < CONTAINER_SIZE; i++) {
                                 initialData.put(p("key_" + i), v("value_" + i));
                             }
                             return client.createMap(key, initialData).get();
                         }, (client, key, hint, threadIdx, i) ->
                                 client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes())
        );
    }

    @Test
    @DisplayName("Stress test: putAll (multi-threaded)")
    void stressTestPutAllMultiThreaded() throws Exception {
        runMultiThreaded("Map.putAll MT", () -> {
                             String key = "stress_m_mt_put_" + UUID.randomUUID();
                             Map<Payload, Variant> initialData = new HashMap<>();
                             for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
                                 initialData.put(p("key_" + i), v("value_" + i));
                             }
                             return client.createMap(key, initialData).get();
                         }, (client, key, hint, threadIdx, i) ->
                                 client.putAll(key, hint, List.of(p("new_key_" + threadIdx + "_" + i)), List.of(v("new_value_" + i)))
        );
    }

    @Test
    @DisplayName("Stress test: update (multi-threaded)")
    void stressTestUpdateMultiThreaded() throws Exception {
        runMultiThreaded("Map.update MT", () -> {
                             String key = "stress_m_mt_update_" + UUID.randomUUID();
                             Map<Payload, Variant> initialData = new HashMap<>();
                             for (int i = 0; i < CONTAINER_SIZE; i++) {
                                 initialData.put(p("key_" + i), v("value_" + i));
                             }
                             return client.createMap(key, initialData).get();
                         }, (client, key, hint, threadIdx, i) ->
                                 client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i))
        );
    }

    @Test
    @DisplayName("Stress test: popInContainer (multi-threaded)")
    void stressTestPopInContainerMultiThreaded() throws Exception {
        runMultiThreaded("Map.popInContainer MT", () -> {
            String key = "stress_m_mt_pop_" + UUID.randomUUID();
            Map<Payload, Variant> initialData = new HashMap<>();
            for (int i = 0; i < CONTAINER_SIZE; i++) {
                initialData.put(p("key_" + i), v("value_" + i));
            }
            return client.createMap(key, initialData).get();
        }, (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_mt_" + threadIdx + "_" + i)), List.of(v("value_" + i)))
                        .exceptionally(e -> 0);
            }
            return client.popInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
        });
    }

    @Test
    @DisplayName("Stress test: remove (multi-threaded)")
    void stressTestRemoveMultiThreaded() throws Exception {
        runMultiThreaded("Map.remove MT", () -> {
            String key = "stress_m_mt_remove_" + UUID.randomUUID();
            Map<Payload, Variant> initialData = new HashMap<>();
            for (int i = 0; i < CONTAINER_SIZE; i++) {
                initialData.put(p("key_" + i), v("value_" + i));
            }
            return client.createMap(key, initialData).get();
        }, (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_mt_" + threadIdx + "_" + i)), List.of(v("value_" + i)))
                        .exceptionally(e -> 0);
            }
            return client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue (multi-threaded)")
    void stressTestRemoveFromContainerByKeyAndValueMultiThreaded() throws Exception {
        runMultiThreaded("Map.removeFromContainerByKeyAndValue MT", () -> {
            String key = "stress_m_mt_remove_val_" + UUID.randomUUID();
            Map<Payload, Variant> initialData = new HashMap<>();
            for (int i = 0; i < CONTAINER_SIZE; i++) {
                initialData.put(p("key_" + i), v("value_" + (i % 100)));
            }
            return client.createMap(key, initialData).get();
        }, (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_mt_" + threadIdx + "_" + i)), List.of(v("value_" + (i % 100))))
                        .exceptionally(e -> 0);
            }
            return client.removeFromContainerByKeyAndValue(
                    key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100))));
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey (multi-threaded)")
    void stressTestRemoveFromContainerByKeyMultiThreaded() throws Exception {
        runMultiThreaded("Map.removeFromContainerByKey MT", () -> {
            String key = "stress_m_mt_remove_key_" + UUID.randomUUID();
            Map<Payload, Variant> initialData = new HashMap<>();
            for (int i = 0; i < CONTAINER_SIZE; i++) {
                initialData.put(p("key_" + i), v("value_" + i));
            }
            return client.createMap(key, initialData).get();
        }, (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_mt_" + threadIdx + "_" + i)), List.of(v("value_" + i)))
                        .exceptionally(e -> 0);
            }
            return client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)));
        });
    }

    // Helper для многопоточных тестов
    @FunctionalInterface
    interface TaskAction {
        CompletableFuture<?> execute(com.hurricache.client.intf.HurriCacheClientInterface client, String key, KeyHintData hint, int threadIdx, int i) throws Exception;
    }

    private void runMultiThreaded(String testName, java.util.concurrent.Callable<KeyHintData> setup, TaskAction action) throws Exception {
        String key = "stress_m_mt_key_" + UUID.randomUUID();
        KeyHintData hint = setup.call();

        int iterationsPerThread = BENCHMARK_ITERATIONS / THREAD_COUNT;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
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

                    for (int i = 0; i < iterationsPerThread; i++) {
                        threadSemaphore.acquire();
                        CompletableFuture<?> future = action.execute(client, key, hint, threadIdx, i);
                        CompletableFuture<?> wrapped = future
                                .exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> threadSemaphore.release());
                        threadFutures.add(wrapped);
                    }
                    CompletableFuture.allOf(threadFutures.toArray(new CompletableFuture[0])).join();
                } catch (Exception e) {
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
        System.out.printf("[%s] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          testName, CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    private void printResults(String operation, int containerSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n",
                          operation, containerSize, iterations, elapsed, ops, errors);
    }
}