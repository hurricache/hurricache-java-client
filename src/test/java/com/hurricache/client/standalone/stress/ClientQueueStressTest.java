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

public class ClientQueueStressTest extends TestBase {

    private static final int WARMUP_ITERATIONS = 5000;
    private static final int BENCHMARK_ITERATIONS = 50000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;
    private static final int CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: peekHead")
    void stressTestPeekHead() throws Exception {
        String key = "stress_q_peek_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + (i % 100)));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.peekHead(key, hint).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.peekHead(key, hint);
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.peekTail(key, hint).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<Variant> future = client.peekTail(key, hint);
            CompletableFuture<Variant> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            client.pushTail(key, hint, List.of(v("item_" + i))).get();
        }

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> future = client.pushTail(key, hint, List.of(v("item_" + idx)));
            CompletableFuture<Integer> wrapped = future
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> semaphore.release());
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> popFuture = client.popHead(key, hint);
            CompletableFuture<Variant> wrapped = popFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.pushTail(key, hint, List.of(v("item_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Variant> popFuture = client.popTail(key, hint);
            CompletableFuture<Variant> wrapped = popFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.pushHead(key, hint, List.of(v("item_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> delFuture = client.deleteHead(key, hint);
            CompletableFuture<Integer> wrapped = delFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.pushTail(key, hint, List.of(v("item_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
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

        int iterations = BENCHMARK_ITERATIONS;
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            final int idx = i;
            CompletableFuture<Integer> delFuture = client.deleteTail(key, hint);
            CompletableFuture<Integer> wrapped = delFuture
                    .exceptionally(ex -> { errors.incrementAndGet(); return null; })
                    .whenComplete((res, ex) -> {
                        if (idx % 10 == 0) {
                            client.pushHead(key, hint, List.of(v("item_" + idx)))
                                    .exceptionally(e -> 0);
                        }
                        semaphore.release();
                    });
            futures.add(wrapped);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.nanoTime();
        printResults("Queue.deleteTail", CONTAINER_SIZE, iterations, endTime - startTime, errors.get());
    }

    @Test
    @DisplayName("Stress test: peekHead (multi-threaded)")
    void stressTestPeekHeadMultiThreaded() throws Exception {
        runQueueMT("Queue.peekHead MT", (client, key, hint, threadIdx, i) -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushHead (multi-threaded)")
    void stressTestPushHeadMultiThreaded() throws Exception {
        runQueueMT("Queue.pushHead MT", (client, key, hint, threadIdx, i) -> client.pushHead(key, hint, List.of(v("item_mt_" + threadIdx + "_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestPushTailMultiThreaded() throws Exception {
        runQueueMT("Queue.pushTail MT", (client, key, hint, threadIdx, i) -> client.pushTail(key, hint, List.of(v("item_mt_" + threadIdx + "_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestPopHeadMultiThreaded() throws Exception {
        runQueueMT("Queue.popHead MT", (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.pushTail(key, hint, List.of(v("item_mt_" + threadIdx + "_" + i))).exceptionally(e -> 0);
            }
            return client.popHead(key, hint);
        });
    }

    @Test
    @DisplayName("Stress test: popTail (multi-threaded)")
    void stressTestPopTailMultiThreaded() throws Exception {
        runQueueMT("Queue.popTail MT", (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.pushHead(key, hint, List.of(v("item_mt_" + threadIdx + "_" + i))).exceptionally(e -> 0);
            }
            return client.popTail(key, hint);
        });
    }

    @Test
    @DisplayName("Stress test: deleteHead (multi-threaded)")
    void stressTestDeleteHeadMultiThreaded() throws Exception {
        runQueueMT("Queue.deleteHead MT", (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.pushTail(key, hint, List.of(v("item_mt_" + threadIdx + "_" + i))).exceptionally(e -> 0);
            }
            return client.deleteHead(key, hint);
        });
    }

    @Test
    @DisplayName("Stress test: deleteTail (multi-threaded)")
    void stressTestDeleteTailMultiThreaded() throws Exception {
        runQueueMT("Queue.deleteTail MT", (client, key, hint, threadIdx, i) -> {
            if (i % 10 == 0) {
                client.pushHead(key, hint, List.of(v("item_mt_" + threadIdx + "_" + i))).exceptionally(e -> 0);
            }
            return client.deleteTail(key, hint);
        });
    }

    @FunctionalInterface
    interface QueueTaskAction {
        CompletableFuture<?> execute(com.hurricache.client.intf.HurriCacheClientInterface client, String key, KeyHintData hint, int threadIdx, int i) throws Exception;
    }

    private void runQueueMT(String testName, QueueTaskAction action) throws Exception {
        String key = "stress_q_mt_" + UUID.randomUUID();
        List<Variant> initialData = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.add(v("item_" + i));
        }
        KeyHintData hint = client.createQueue(key, initialData).get();

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