package com.hurricache.client.standalone.stress;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ClientVectorStressTest extends TestBase {

    private static final int WARMUP_ITERATIONS = 5000;
    private static final int BENCHMARK_ITERATIONS = 50000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: getVector")
    void stressTestGetVector() throws Exception {
        String key = "stress_v_get_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + (i % 100)));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<List<Variant>> future = client.getVector(key, hint);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Vector.getVector", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: getAt")
    void stressTestGetAt() throws Exception {
        String key = "stress_v_at_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.getAt(key, hint, i % CONTAINER_SIZE).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> future = client.getAt(key, hint, idx % CONTAINER_SIZE);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Vector.getAt", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: popAt")
    void stressTestPopAt() throws Exception {
        String key = "stress_v_pop_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.popAt(key, hint, i % CONTAINER_SIZE).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> future = client.popAt(key, hint, idx % CONTAINER_SIZE);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Vector.popAt", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "stress_v_insert_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.insertAt(key, hint, List.of(v("new_" + i)), i % CONTAINER_SIZE).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> future = client.insertAt(key, hint, List.of(v("new_" + idx)), idx % CONTAINER_SIZE);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Vector.insertAt", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "stress_v_remove_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.removeAt(key, hint, i % CONTAINER_SIZE).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> future = client.removeAt(key, hint, idx % CONTAINER_SIZE);
            futures.add(future.exceptionally(ex -> { errors.incrementAndGet(); return null; })
                                .whenComplete((res, ex) -> semaphore.release()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Vector.removeAt", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestVectorGetSize() throws Exception {
        String key = "stress_v_size_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

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
        printResults("Vector.getSize", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: getAt (multi-threaded)")
    void stressTestGetAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

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
                        CompletableFuture<Variant> future = client.getAt(key, hint, i % CONTAINER_SIZE);
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
        System.out.printf("[Vector.getAt MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    @Test
    @DisplayName("Stress test: popAt (multi-threaded)")
    void stressTestPopAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_pop_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

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
                        CompletableFuture<Variant> future = client.popAt(key, hint, i % CONTAINER_SIZE);
                        threadFutures.add(future.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                  .whenComplete((res, ex) -> threadSemaphore.release()));

                        if (i % 10 == 0) {
                            threadSemaphore.acquire();
                            final int uniqueId = threadIdx * iterationsPerThread + i;
                            CompletableFuture<Integer> pushFuture = client.pushTail(key, hint, List.of(v("item_mt_" + uniqueId)));
                            threadFutures.add(pushFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
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
        System.out.printf("[Vector.popAt MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    @Test
    @DisplayName("Stress test: insertAt (multi-threaded)")
    void stressTestVectorInsertAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_insert_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

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
                        CompletableFuture<Integer> future = client.insertAt(key, hint, List.of(v("new_" + uniqueId)), i % CONTAINER_SIZE);
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
        System.out.printf("[Vector.insertAt MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    @Test
    @DisplayName("Stress test: removeAt (multi-threaded)")
    void stressTestVectorRemoveAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_remove_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createVector(key, initialData).get();

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
                        CompletableFuture<Integer> future = client.removeAt(key, hint, i % CONTAINER_SIZE);
                        threadFutures.add(future.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
                                                  .whenComplete((res, ex) -> threadSemaphore.release()));

                        if (i % 10 == 0) {
                            threadSemaphore.acquire();
                            final int uniqueId = threadIdx * iterationsPerThread + i;
                            CompletableFuture<Integer> pushFuture = client.pushTail(key, hint, List.of(v("item_mt_" + uniqueId)));
                            threadFutures.add(pushFuture.exceptionally(ex -> { totalErrors.incrementAndGet(); return null; })
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
        System.out.printf("[Vector.removeAt MT] size=%d, %d iters x %d threads, %.3fs, %.2f OPS, %d errors%n",
                          CONTAINER_SIZE, iterationsPerThread, THREAD_COUNT, elapsed, ops, totalErrors.get());
    }

    private void printResults(String operation, int containerSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n",
                          operation, containerSize, iterations, elapsed, ops, errors);
    }
}