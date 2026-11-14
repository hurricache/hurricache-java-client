package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * Common base class for all cluster stress tests.
 * Provides unified, DRY execution engines for single-threaded and multi-threaded benchmarks.
 */
public abstract class BaseClusterStressTest extends TestBaseCluster {

    protected static final int WARMUP_ITERATIONS = 10000;
    protected static final int BENCHMARK_ITERATIONS = 100000;
    protected static final int CONCURRENCY = 100;
    protected static final int THREAD_COUNT = 4;
    protected static final int CONTAINER_SIZE = 1000;

    protected static ExecutorService threadPool;

    @BeforeAll
    static void initThreadPool() {
        threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterAll
    static void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {
        R apply(T t) throws Exception;
    }

    /**
     * Executes warmup iterations without recording metrics.
     */
    protected void runWarmup(int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        for (int i = 0; i < iterations; i++) {
            CompletableFuture<?> f = action.apply(i);
            if (f != null) {
                f.get();
            }
        }
    }

    /**
     * Standard single-threaded benchmark engine with concurrency control via Semaphore.
     */
    protected void runBenchmark(String name, int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        runBenchmark(name, -1, iterations, action);
    }

    protected void runBenchmark(String name, int containerSize, int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<?> f;
            try {
                f = action.apply(i);
            } catch (Exception e) {
                semaphore.release();
                errors.incrementAndGet();
                continue;
            }

            if (f != null) {
                futures.add(f.whenComplete((res, ex) -> {
                    semaphore.release();
                    if (ex != null) errors.incrementAndGet();
                }));
            } else {
                semaphore.release();
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        printResults(name, containerSize, iterations, System.nanoTime() - start, errors.get());
    }

    /**
     * Standard multi-threaded benchmark engine using fixed thread pools.
     */
    protected void runMultiThreadedBenchmark(String name, int totalIterations, int threads, BiFunction<Integer, Integer, CompletableFuture<?>> action) throws Exception {
        runMultiThreadedBenchmark(name, -1, totalIterations, threads, action);
    }

    protected void runMultiThreadedBenchmark(String name, int containerSize, int totalIterations, int threads, BiFunction<Integer, Integer, CompletableFuture<?>> action) throws Exception {
        int perThread = totalIterations / threads;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>(totalIterations));

        long start = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            threadPool.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        semaphore.acquire();
                        CompletableFuture<?> f;
                        try {
                            f = action.apply(threadId, i);
                        } catch (Exception e) {
                            semaphore.release();
                            errors.incrementAndGet();
                            continue;
                        }

                        if (f != null) {
                            futures.add(f.whenComplete((res, ex) -> {
                                semaphore.release();
                                if (ex != null) errors.incrementAndGet();
                            }));
                        } else {
                            semaphore.release();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        printResults(name + " MT", containerSize, totalIterations, System.nanoTime() - start, errors.get());
    }

    // Helpers for test data generation
    protected Map<Payload, Variant> generateMapData(int size) {
        Map<Payload, Variant> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(p("key_" + i), v("value_" + i));
        }
        return map;
    }

    protected List<Variant> generateListData(int size) {
        List<Variant> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(v("item_" + (i % 100)));
        }
        return list;
    }

    protected void printResults(String operation, int containerSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        if (containerSize > 0) {
            System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n", operation, containerSize, iterations, elapsed, ops, errors);
        } else {
            System.out.printf("[%s] %d iters, %.3fs, %.2f OPS, %d errors%n", operation, iterations, elapsed, ops, errors);
        }
    }
}