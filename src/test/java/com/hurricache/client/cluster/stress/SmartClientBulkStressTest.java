package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.StatusRuntimeException;
import io.grpc.Status;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive stress test for bulk create, read, and update operations
 * with different data types (long, double, byte array).
 *
 * Tests parallel create, parallel read, and parallel update scenarios.
 * Supports both with-hint and without-hint modes.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class SmartClientBulkStressTest extends TestBaseCluster {

    // ====== CONFIGURABLE PARAMETERS ======
    private static final int DATA_SIZE = 400000;
    private static final int KEY_LENGTH = 128;                     // Длина ключа
    private static final int BYTE_ARRAY_SIZE = 256;               // Размер массива байт в байтах
    private static final int THREAD_COUNT = 4;
    private static final int MAX_PARALLEL_REQUESTS_PER_THREAD = 32; // Контроль in-flight запросов
    private static final int WARMUP_ITERATIONS = 1000;
    private static final String TEST_RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    private String generateKey(int idx) {
        String base = TEST_RUN_ID + "_" + idx + "_";
        if (base.length() >= KEY_LENGTH) {
            return base.substring(0, KEY_LENGTH);
        }
        StringBuilder sb = new StringBuilder(base);
        while (sb.length() < KEY_LENGTH) {
            sb.append('a');
        }
        return sb.toString();
    }

    private byte[] generateByteArray(int idx) {
        byte[] data = new byte[BYTE_ARRAY_SIZE];
        byte fillVal = (byte) (idx % 127);
        Arrays.fill(data, fillVal);
        return data;
    }

    private Variant createVariant(int type, int idx) {
        switch (type) {
            case 0: return vi(idx);
            case 1: return vd(idx * 1.5);
            case 2: return v(generateByteArray(idx));
            default: throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    private void waitForReplication() throws InterruptedException {
        Thread.sleep(500);
    }

    /**
     * Check if exception is expected in concurrent environment:
     * - ALREADY_EXISTS: key was created by another thread
     * - NOT_FOUND: key was deleted by another thread
     */
    private boolean isExpectedError(Throwable ex) {
        if (ex instanceof StatusRuntimeException sre) {
            Status.Code code = sre.getStatus().getCode();
            return code == Status.Code.ALREADY_EXISTS || code == Status.Code.NOT_FOUND;
        }
        return false;
    }

    // =========================================================================
    // PARALLEL CREATE TESTS (WITHOUT HINT)
    // =========================================================================

    @Test
    @DisplayName("Bulk stress test: parallel create (long, without hint)")
    void bulkTestParallelCreateLongWithoutHint() throws Exception {
        String prefix = "bcl_";
        AtomicLong expectedErrors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;
                        String key = prefix + generateKey(globalIdx);

                        semaphore.acquire();
                        CompletableFuture<KeyHintData> fut = client.create(key, vi(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                expectedErrors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        waitForReplication();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Create Long (no hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, expectedErrors.get());
    }

    @Test
    @DisplayName("Bulk stress test: parallel create (double, without hint)")
    void bulkTestParallelCreateDoubleWithoutHint() throws Exception {
        String prefix = "bcd_";
        AtomicLong expectedErrors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;
                        String key = prefix + generateKey(globalIdx);

                        semaphore.acquire();
                        CompletableFuture<KeyHintData> fut = client.create(key, vd(globalIdx * 1.5));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                expectedErrors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        waitForReplication();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Create Double (no hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, expectedErrors.get());
    }

    @Test
    @DisplayName("Bulk stress test: parallel create (bytes, without hint)")
    void bulkTestParallelCreateBytesWithoutHint() throws Exception {
        String prefix = "bcb_";
        AtomicLong expectedErrors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;
                        String key = prefix + generateKey(globalIdx);

                        semaphore.acquire();
                        CompletableFuture<KeyHintData> fut = client.create(key, v(generateByteArray(globalIdx)));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                expectedErrors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        waitForReplication();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Create Bytes (no hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, expectedErrors.get());
    }

    // =========================================================================
    // PARALLEL READ TESTS (WITH HINT)
    // =========================================================================

    @Test
    @DisplayName("Bulk stress test: parallel read (long, with hint)")
    void bulkTestParallelReadLongWithHint() throws Exception {
        String prefix = "brl_";
        List<String> keys = new ArrayList<>(DATA_SIZE);
        List<KeyHintData> hints = new ArrayList<>(DATA_SIZE);

        for (int i = 0; i < DATA_SIZE; i++) {
            String key = prefix + generateKey(i);
            KeyHintData hint = client.create(key, vi(i)).get();
            keys.add(key);
            hints.add(hint);
        }
        waitForReplication();

        AtomicLong errors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.get(keys.get(globalIdx), hints.get(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                errors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Read Long (with hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, errors.get());
    }

    @Test
    @DisplayName("Bulk stress test: parallel read (double, with hint)")
    void bulkTestParallelReadDoubleWithHint() throws Exception {
        String prefix = "brd_";
        List<String> keys = new ArrayList<>(DATA_SIZE);
        List<KeyHintData> hints = new ArrayList<>(DATA_SIZE);

        for (int i = 0; i < DATA_SIZE; i++) {
            String key = prefix + generateKey(i);
            KeyHintData hint = client.create(key, vd(i * 1.5)).get();
            keys.add(key);
            hints.add(hint);
        }
        waitForReplication();

        AtomicLong errors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.get(keys.get(globalIdx), hints.get(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                errors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Read Double (with hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, errors.get());
    }

    @Test
    @DisplayName("Bulk stress test: parallel read (bytes, with hint)")
    void bulkTestParallelReadBytesWithHint() throws Exception {
        String prefix = "brb_";
        List<String> keys = new ArrayList<>(DATA_SIZE);
        List<KeyHintData> hints = new ArrayList<>(DATA_SIZE);

        for (int i = 0; i < DATA_SIZE; i++) {
            String key = prefix + generateKey(i);
            KeyHintData hint = client.create(key, v(generateByteArray(i))).get();
            keys.add(key);
            hints.add(hint);
        }
        waitForReplication();

        AtomicLong errors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.get(keys.get(globalIdx), hints.get(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                errors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Read Bytes (with hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, errors.get());
    }

    // =========================================================================
    // PARALLEL UPDATE TESTS (WITH HINT)
    // =========================================================================

    @Test
    @DisplayName("Bulk stress test: parallel update (long, with hint)")
    void bulkTestParallelUpdateLongWithHint() throws Exception {
        String prefix = "bul_";
        List<String> keys = new ArrayList<>(DATA_SIZE);
        List<KeyHintData> hints = new ArrayList<>(DATA_SIZE);

        for (int i = 0; i < DATA_SIZE; i++) {
            String key = prefix + generateKey(i);
            KeyHintData hint = client.create(key, vi(i)).get();
            keys.add(key);
            hints.add(hint);
        }
        waitForReplication();

        AtomicLong errors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.replace(keys.get(globalIdx), hints.get(globalIdx), vi(globalIdx * 2));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                errors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Update Long (with hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, errors.get());
    }

    @Test
    @DisplayName("Bulk stress test: parallel update (double, with hint)")
    void bulkTestParallelUpdateDoubleWithHint() throws Exception {
        String prefix = "bud_";
        List<String> keys = new ArrayList<>(DATA_SIZE);
        List<KeyHintData> hints = new ArrayList<>(DATA_SIZE);

        for (int i = 0; i < DATA_SIZE; i++) {
            String key = prefix + generateKey(i);
            KeyHintData hint = client.create(key, vd(i * 1.5)).get();
            keys.add(key);
            hints.add(hint);
        }
        waitForReplication();

        AtomicLong errors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.replace(keys.get(globalIdx), hints.get(globalIdx), vd(globalIdx * 2.5));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                errors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Update Double (with hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, errors.get());
    }

    @Test
    @DisplayName("Bulk stress test: parallel update (bytes, with hint)")
    void bulkTestParallelUpdateBytesWithHint() throws Exception {
        String prefix = "bub_";
        List<String> keys = new ArrayList<>(DATA_SIZE);
        List<KeyHintData> hints = new ArrayList<>(DATA_SIZE);

        for (int i = 0; i < DATA_SIZE; i++) {
            String key = prefix + generateKey(i);
            KeyHintData hint = client.create(key, v(generateByteArray(i))).get();
            keys.add(key);
            hints.add(hint);
        }
        waitForReplication();

        AtomicLong errors = new AtomicLong(0);
        int iterationsPerThread = DATA_SIZE / THREAD_COUNT;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] futures = new CompletableFuture[DATA_SIZE];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        int globalIdx = threadIdx * iterationsPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.replace(keys.get(globalIdx), hints.get(globalIdx), v(generateByteArray(globalIdx + 1)));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                errors.incrementAndGet();
                            }
                        });
                        futures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            bt.start();
        }

        startLatch.countDown();
        doneLatch.await();
        CompletableFuture.allOf(futures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        double ops = DATA_SIZE / elapsed;
        System.out.printf("[Bulk Update Bytes (with hint)] size=%d, %d iters, %.3fs, %.2f OPS, %d unexpected errors%n",
                          DATA_SIZE, DATA_SIZE, elapsed, ops, errors.get());
    }

    // =========================================================================
    // MIXED WORKLOAD TESTS (CREATE + READ + UPDATE)
    // =========================================================================

    @Test
    @DisplayName("Bulk stress test: mixed workload (create + read + update, long)")
    void bulkTestMixedWorkloadLong() throws Exception {
        String prefix = "bml_";
        ConcurrentLinkedQueue<String> keysQueue = new ConcurrentLinkedQueue<>();

        // Phase 1: Parallel create
        int createCountPerThread = (DATA_SIZE / 3) / THREAD_COUNT;
        int totalCreateCount = createCountPerThread * THREAD_COUNT;

        CountDownLatch createStartLatch = new CountDownLatch(1);
        CountDownLatch createDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong createErrors = new AtomicLong(0);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] createFutures = new CompletableFuture[totalCreateCount];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    createStartLatch.await();
                    for (int i = 0; i < createCountPerThread; i++) {
                        int globalIdx = threadIdx * createCountPerThread + i;
                        String key = prefix + generateKey(globalIdx);
                        keysQueue.add(key);

                        semaphore.acquire();
                        CompletableFuture<KeyHintData> fut = client.create(key, vi(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                createErrors.incrementAndGet();
                            }
                        });
                        createFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    createDoneLatch.countDown();
                }
            });
            bt.start();
        }

        createStartLatch.countDown();
        createDoneLatch.await();
        CompletableFuture.allOf(createFutures).join();

        List<String> keys = new ArrayList<>(keysQueue);
        waitForReplication();

        // Phase 2: Parallel read
        CountDownLatch readStartLatch = new CountDownLatch(1);
        CountDownLatch readDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong readErrors = new AtomicLong(0);
        int readPerThread = keys.size() / THREAD_COUNT;
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] readFutures = new CompletableFuture[readPerThread * THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    readStartLatch.await();
                    for (int i = 0; i < readPerThread; i++) {
                        int globalIdx = threadIdx * readPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.get(keys.get(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                readErrors.incrementAndGet();
                            }
                        });
                        readFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    readDoneLatch.countDown();
                }
            });
            bt.start();
        }

        readStartLatch.countDown();
        readDoneLatch.await();
        CompletableFuture.allOf(readFutures).join();
        waitForReplication();

        // Phase 3: Parallel update
        CountDownLatch updateStartLatch = new CountDownLatch(1);
        CountDownLatch updateDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong updateErrors = new AtomicLong(0);
        int updatePerThread = keys.size() / THREAD_COUNT;
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] updateFutures = new CompletableFuture[updatePerThread * THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    updateStartLatch.await();
                    for (int i = 0; i < updatePerThread; i++) {
                        int globalIdx = threadIdx * updatePerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.replace(keys.get(globalIdx), vi(keys.size() + globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                updateErrors.incrementAndGet();
                            }
                        });
                        updateFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    updateDoneLatch.countDown();
                }
            });
            bt.start();
        }

        updateStartLatch.countDown();
        updateDoneLatch.await();
        CompletableFuture.allOf(updateFutures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        long totalOps = totalCreateCount + readFutures.length + updateFutures.length;
        double ops = totalOps / elapsed;
        System.out.printf("[Bulk Mixed Long] create=%d, read=%d, update=%d, %.3fs, %.2f OPS, createErr=%d, readErr=%d, updateErr=%d%n",
                          totalCreateCount, readFutures.length, updateFutures.length, elapsed, ops,
                          createErrors.get(), readErrors.get(), updateErrors.get());
    }

    @Test
    @DisplayName("Bulk stress test: mixed workload (create + read + update, double)")
    void bulkTestMixedWorkloadDouble() throws Exception {
        String prefix = "bmd_";
        ConcurrentLinkedQueue<String> keysQueue = new ConcurrentLinkedQueue<>();

        // Phase 1: Parallel create
        int createCountPerThread = (DATA_SIZE / 3) / THREAD_COUNT;
        int totalCreateCount = createCountPerThread * THREAD_COUNT;

        CountDownLatch createStartLatch = new CountDownLatch(1);
        CountDownLatch createDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong createErrors = new AtomicLong(0);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] createFutures = new CompletableFuture[totalCreateCount];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    createStartLatch.await();
                    for (int i = 0; i < createCountPerThread; i++) {
                        int globalIdx = threadIdx * createCountPerThread + i;
                        String key = prefix + generateKey(globalIdx);
                        keysQueue.add(key);

                        semaphore.acquire();
                        CompletableFuture<KeyHintData> fut = client.create(key, vd(globalIdx * 1.5));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                createErrors.incrementAndGet();
                            }
                        });
                        createFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    createDoneLatch.countDown();
                }
            });
            bt.start();
        }

        createStartLatch.countDown();
        createDoneLatch.await();
        CompletableFuture.allOf(createFutures).join();

        List<String> keys = new ArrayList<>(keysQueue);
        waitForReplication();

        // Phase 2: Parallel read
        CountDownLatch readStartLatch = new CountDownLatch(1);
        CountDownLatch readDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong readErrors = new AtomicLong(0);
        int readPerThread = keys.size() / THREAD_COUNT;
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] readFutures = new CompletableFuture[readPerThread * THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    readStartLatch.await();
                    for (int i = 0; i < readPerThread; i++) {
                        int globalIdx = threadIdx * readPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.get(keys.get(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                readErrors.incrementAndGet();
                            }
                        });
                        readFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    readDoneLatch.countDown();
                }
            });
            bt.start();
        }

        readStartLatch.countDown();
        readDoneLatch.await();
        CompletableFuture.allOf(readFutures).join();
        waitForReplication();

        // Phase 3: Parallel update
        CountDownLatch updateStartLatch = new CountDownLatch(1);
        CountDownLatch updateDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong updateErrors = new AtomicLong(0);
        int updatePerThread = keys.size() / THREAD_COUNT;
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] updateFutures = new CompletableFuture[updatePerThread * THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    updateStartLatch.await();
                    for (int i = 0; i < updatePerThread; i++) {
                        int globalIdx = threadIdx * updatePerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.replace(keys.get(globalIdx), vd(keys.size() * 2.5 + globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                updateErrors.incrementAndGet();
                            }
                        });
                        updateFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    updateDoneLatch.countDown();
                }
            });
            bt.start();
        }

        updateStartLatch.countDown();
        updateDoneLatch.await();
        CompletableFuture.allOf(updateFutures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        long totalOps = totalCreateCount + readFutures.length + updateFutures.length;
        double ops = totalOps / elapsed;
        System.out.printf("[Bulk Mixed Double] create=%d, read=%d, update=%d, %.3fs, %.2f OPS, createErr=%d, readErr=%d, updateErr=%d%n",
                          totalCreateCount, readFutures.length, updateFutures.length, elapsed, ops,
                          createErrors.get(), readErrors.get(), updateErrors.get());
    }

    @Test
    @DisplayName("Bulk stress test: mixed workload (create + read + update, bytes)")
    void bulkTestMixedWorkloadBytes() throws Exception {
        String prefix = "bmb_";
        ConcurrentLinkedQueue<String> keysQueue = new ConcurrentLinkedQueue<>();

        // Phase 1: Parallel create
        int createCountPerThread = (DATA_SIZE / 3) / THREAD_COUNT;
        int totalCreateCount = createCountPerThread * THREAD_COUNT;

        CountDownLatch createStartLatch = new CountDownLatch(1);
        CountDownLatch createDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong createErrors = new AtomicLong(0);
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] createFutures = new CompletableFuture[totalCreateCount];

        long startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    createStartLatch.await();
                    for (int i = 0; i < createCountPerThread; i++) {
                        int globalIdx = threadIdx * createCountPerThread + i;
                        String key = prefix + generateKey(globalIdx);
                        keysQueue.add(key);

                        semaphore.acquire();
                        CompletableFuture<KeyHintData> fut = client.create(key, v(generateByteArray(globalIdx)));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                createErrors.incrementAndGet();
                            }
                        });
                        createFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    createDoneLatch.countDown();
                }
            });
            bt.start();
        }

        createStartLatch.countDown();
        createDoneLatch.await();
        CompletableFuture.allOf(createFutures).join();

        List<String> keys = new ArrayList<>(keysQueue);
        waitForReplication();

        // Phase 2: Parallel read
        CountDownLatch readStartLatch = new CountDownLatch(1);
        CountDownLatch readDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong readErrors = new AtomicLong(0);
        int readPerThread = keys.size() / THREAD_COUNT;
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] readFutures = new CompletableFuture[readPerThread * THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    readStartLatch.await();
                    for (int i = 0; i < readPerThread; i++) {
                        int globalIdx = threadIdx * readPerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.get(keys.get(globalIdx));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                readErrors.incrementAndGet();
                            }
                        });
                        readFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    readDoneLatch.countDown();
                }
            });
            bt.start();
        }

        readStartLatch.countDown();
        readDoneLatch.await();
        CompletableFuture.allOf(readFutures).join();
        waitForReplication();

        // Phase 3: Parallel update
        CountDownLatch updateStartLatch = new CountDownLatch(1);
        CountDownLatch updateDoneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicLong updateErrors = new AtomicLong(0);
        int updatePerThread = keys.size() / THREAD_COUNT;
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] updateFutures = new CompletableFuture[updatePerThread * THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            Thread bt = new Thread(() -> {
                Semaphore semaphore = new Semaphore(MAX_PARALLEL_REQUESTS_PER_THREAD);
                try {
                    updateStartLatch.await();
                    for (int i = 0; i < updatePerThread; i++) {
                        int globalIdx = threadIdx * updatePerThread + i;

                        semaphore.acquire();
                        CompletableFuture<Variant> fut = client.replace(keys.get(globalIdx), v(generateByteArray(globalIdx + 2)));
                        fut.whenComplete((res, ex) -> {
                            semaphore.release();
                            if (ex != null && !isExpectedError(ex)) {
                                updateErrors.incrementAndGet();
                            }
                        });
                        updateFutures[globalIdx] = fut;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    updateDoneLatch.countDown();
                }
            });
            bt.start();
        }

        updateStartLatch.countDown();
        updateDoneLatch.await();
        CompletableFuture.allOf(updateFutures).join();
        long endTime = System.nanoTime();

        double elapsed = (endTime - startTime) / 1_000_000_000.0;
        long totalOps = totalCreateCount + readFutures.length + updateFutures.length;
        double ops = totalOps / elapsed;
        System.out.printf("[Bulk Mixed Bytes] create=%d, read=%d, update=%d, %.3fs, %.2f OPS, createErr=%d, readErr=%d, updateErr=%d%n",
                          totalCreateCount, readFutures.length, updateFutures.length, elapsed, ops,
                          createErrors.get(), readErrors.get(), updateErrors.get());
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    private void printResults(String operation, int dataSize, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] size=%d, %d iters, %.3fs, %.2f OPS, %d errors%n",
                          operation, dataSize, iterations, elapsed, ops, errors);
    }

    @Override
    protected Duration getTestTtl() {
        return Duration.ofMinutes(15);
    }
}