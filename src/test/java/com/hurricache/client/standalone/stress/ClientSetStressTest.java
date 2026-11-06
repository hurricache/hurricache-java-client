package com.hurricache.client.standalone.stress;

import com.hurricache.TestBase;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ClientSetStressTest extends TestBase {

    private static final int WARMUP_ITERATIONS = 5000;
    private static final int BENCHMARK_ITERATIONS = 50000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 1000;

    @Test
    @DisplayName("Stress test: getSet")
    void stressTestGetSet() throws Exception {
        String key = "stress_s_get_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getSet(key, hint).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<List<Variant>> future = client.getSet(key, hint);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.addElementUnordered(key, hint, List.of(v("new_" + i))).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> future = client.addElementUnordered(key, hint, List.of(v("new_" + idx)));
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes()).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final byte[] targetKey = ("item_" + (i % CONTAINER_SIZE)).getBytes();
            CompletableFuture<Boolean> future = client.containsKey(key, hint, targetKey);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE)))).get();
            if (i % 10 == 0) {
                client.addElementUnordered(key, hint, List.of(v("item_" + i))).get();
            }
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(
                    key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (idx % CONTAINER_SIZE))));
            futures.add(delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));

            if (idx % 10 == 0) {
                semaphore.acquire();
                CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + idx)));
                futures.add(addFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                    .whenComplete((res, ex) -> semaphore.release()));
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE))).get();
            if (i % 10 == 0) {
                client.addElementUnordered(key, hint, List.of(v("item_" + i))).get();
            }
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, v("item_" + (idx % CONTAINER_SIZE)));
            futures.add(delFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));

            if (idx % 10 == 0) {
                semaphore.acquire();
                CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + idx)));
                futures.add(addFuture.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                    .whenComplete((res, ex) -> semaphore.release()));
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getSize(key, hint).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Integer> future = client.getSize(key, hint);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Set.getSize", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: containsKey (multi-threaded)")
    void stressTestContainsKeyMultiThreaded() throws Exception {
        String key = "stress_s_mt_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();

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
                        final byte[] targetKey = ("item_" + (i % CONTAINER_SIZE)).getBytes();
                        CompletableFuture<Boolean> future = client.containsKey(key, hint, targetKey);
                        threadFutures.add(future.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                  .whenComplete((res, ex) -> threadSemaphore.release()));
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
        System.out.printf("[Set.containsKey MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    @Test
    @DisplayName("Stress test: addElementUnordered (multi-threaded)")
    void stressTestAddElementUnorderedMultiThreaded() throws Exception {
        String key = "stress_s_mt_add_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE / 10; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createSet(key, initialData).get();

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
                        final int uniqueId = threadIdx * iterationsPerThread + i;
                        CompletableFuture<Integer> future = client.addElementUnordered(key, hint, List.of(v("new_" + uniqueId)));
                        threadFutures.add(future.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                  .whenComplete((res, ex) -> threadSemaphore.release()));
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
                        final int idx = i;
                        CompletableFuture<Integer> delFuture = client.removeFromContainerByKeyAndValue(
                                key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (idx % CONTAINER_SIZE))));
                        threadFutures.add(delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                  .whenComplete((res, ex) -> threadSemaphore.release()));

                        if (idx % 10 == 0) {
                            threadSemaphore.acquire();
                            final int uniqueId = threadIdx * iterationsPerThread + idx;
                            CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + uniqueId)));
                            threadFutures.add(addFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                      .whenComplete((res, ex) -> threadSemaphore.release()));
                        }
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
                        final int idx = i;
                        CompletableFuture<Integer> delFuture = client.removeFromContainerByKey(key, hint, v("item_" + (idx % CONTAINER_SIZE)));
                        threadFutures.add(delFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                  .whenComplete((res, ex) -> threadSemaphore.release()));

                        if (idx % 10 == 0) {
                            threadSemaphore.acquire();
                            final int uniqueId = threadIdx * iterationsPerThread + idx;
                            CompletableFuture<Integer> addFuture = client.addElementUnordered(key, hint, List.of(v("item_" + uniqueId)));
                            threadFutures.add(addFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                      .whenComplete((res, ex) -> threadSemaphore.release()));
                        }
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
        System.out.printf("[Set.removeFromContainerByKey MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    private void printResults(String operation, int containerSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n",
                          operation, containerSize, iterations, elapsed, ops, errors);
    }
}