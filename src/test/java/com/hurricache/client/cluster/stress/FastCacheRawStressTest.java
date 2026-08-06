package com.hurricache.client.cluster.stress;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FastCacheRawStressTest {

    private final String prefix = UUID.randomUUID() + "-" + System.currentTimeMillis() + ":::";
    private final int THREAD_COUNT = 1;
    private final int OPERATIONS_PER_THREAD = 100_0000;
    private final int PIPELINE_BATCH_SIZE = 512;
    private final int EXPECTED_TOTAL_OPS = THREAD_COUNT * OPERATIONS_PER_THREAD;
    private final int BATCH_TIMEOUT_SECONDS = 10;

    private static final byte[] PREALLOCATED_VALUE = "value_data_payload_placeholder_for_stress_testing".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREALLOCATED_UPDATE = "value_data_payload_placeholder_for_stress_testing_updated".getBytes(StandardCharsets.UTF_8);

    private FastCacheAsyncSmartClient client;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws InterruptedException {
        executor = Executors.newFixedThreadPool(THREAD_COUNT);

        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(5);
            }
        };
        client.setMode(Mode.MASTER_THAN_BACKUP);

        while (!client.getReadyFlag()) {
            Thread.sleep(100);
        }
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void highConcurrencyCreateLoadTest() throws InterruptedException {
        KeyHintData[][] threadLocalStorage = new KeyHintData[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // ==========================================
        // 1. PIPELINED WRITE SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Writes...");
        CountDownLatch writeLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger writeSuccessCount = new AtomicInteger(0);
        AtomicInteger writeErrorCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHintData>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey(threadId, j);

                        pipeline.add(client.createKeyValue(key, PREALLOCATED_VALUE));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processWriteBatch(pipeline, indexMapping, threadLocalStorage[threadId], writeSuccessCount, writeErrorCount);
                            pipeline.clear();
                            indexMapping.clear();
                        }
                    }
                } finally {
                    writeLatch.countDown();
                }
            });
        }

        writeLatch.await(10, TimeUnit.MINUTES);
        printResults("Write", startTime, writeSuccessCount.get(), writeErrorCount.get());

        // ==========================================
        // 2. PIPELINED READ SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Reads...");
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readErrorCount = new AtomicInteger(0);

        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey(threadId, j);
                        KeyHintData hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.getValue(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processReadBatch(pipeline, readSuccessCount, readErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }

        readLatch.await(10, TimeUnit.MINUTES);
        printResults("Read", startTime, readSuccessCount.get(), readErrorCount.get());

        // ==========================================
        // 3. PIPELINED UPDATE SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Updates...");
        CountDownLatch updateLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger updateSuccessCount = new AtomicInteger(0);
        AtomicInteger updateErrorCount = new AtomicInteger(0);

        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey(threadId, j);
                        KeyHintData hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.updateKeyValue(key, hint, PREALLOCATED_UPDATE));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processReadBatch(pipeline, updateSuccessCount, updateErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    updateLatch.countDown();
                }
            });
        }

        updateLatch.await(10, TimeUnit.MINUTES);
        printResults("Update", startTime, updateSuccessCount.get(), updateErrorCount.get());

        // ==========================================
        // 4. PIPELINED DELETE SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Deletes...");
        CountDownLatch deleteLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger deleteSuccessCount = new AtomicInteger(0);
        AtomicInteger deleteErrorCount = new AtomicInteger(0);

        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey(threadId, j);
                        KeyHintData hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.remove(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processBooleanBatch(pipeline, deleteSuccessCount, deleteErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    deleteLatch.countDown();
                }
            });
        }

        deleteLatch.await(10, TimeUnit.MINUTES);
        printResults("Delete", startTime, deleteSuccessCount.get(), deleteErrorCount.get());

        // Validations
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, writeSuccessCount.get(), "Write validation mismatch");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, readSuccessCount.get(), "Read validation mismatch");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, updateSuccessCount.get(), "Update validation mismatch");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, deleteSuccessCount.get(), "Delete validation mismatch");
    }

    private String getTestKey(int threadId, int opId) {
        return prefix + "stress:" + threadId + ":" + opId;
    }

    // =========================================================================
    // HELPER BATCH PROCESSORS
    // =========================================================================

    private void processWriteBatch(List<CompletableFuture<KeyHintData>> pipeline,
                                   List<Integer> indexMapping,
                                   KeyHintData[] threadStorage,
                                   AtomicInteger successCounter,
                                   AtomicInteger errorCounter) {
        try {
            CompletableFuture<Void> allOf = CompletableFuture.allOf(pipeline.toArray(new CompletableFuture[0]));
            allOf.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (int k = 0; k < pipeline.size(); k++) {
                KeyHintData hint = pipeline.get(k).getNow(null);
                if (hint != null) {
                    threadStorage[indexMapping.get(k)] = hint;
                    successCounter.incrementAndGet();
                } else {
                    errorCounter.incrementAndGet();
                }
            }
        } catch (Exception e) {
            errorCounter.addAndGet(pipeline.size());
        }
    }

    private void processReadBatch(List<CompletableFuture<byte[]>> pipeline,
                                  AtomicInteger successCounter,
                                  AtomicInteger errorCounter) {
        try {
            CompletableFuture<Void> allOf = CompletableFuture.allOf(pipeline.toArray(new CompletableFuture[0]));
            allOf.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (CompletableFuture<byte[]> future : pipeline) {
                byte[] data = future.getNow(null);
                if (data != null) {
                    successCounter.incrementAndGet();
                } else {
                    errorCounter.incrementAndGet();
                }
            }
        } catch (Exception e) {
            errorCounter.addAndGet(pipeline.size());
        }
    }

    private void processBooleanBatch(List<CompletableFuture<Boolean>> pipeline,
                                     AtomicInteger successCounter,
                                     AtomicInteger errorCounter) {
        try {
            CompletableFuture<Void> allOf = CompletableFuture.allOf(pipeline.toArray(new CompletableFuture[0]));
            allOf.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (CompletableFuture<Boolean> future : pipeline) {
                if (Boolean.TRUE.equals(future.getNow(false))) {
                    successCounter.incrementAndGet();
                } else {
                    errorCounter.incrementAndGet();
                }
            }
        } catch (Exception e) {
            errorCounter.addAndGet(pipeline.size());
        }
    }

    private void printResults(String operationalPhase, long startTime, int successCount, int errorCount) {
        long duration = System.currentTimeMillis() - startTime;
        double opsPerSec = (double) (successCount + errorCount) / (duration / 1000.0);
        System.out.println(String.format("--- Stress Test %s Results ---", operationalPhase));
        System.out.println("Total Operations: " + (successCount + errorCount));
        System.out.println("Successes: " + successCount);
        System.out.println("Errors: " + errorCount);
        System.out.println("Duration: " + duration + " ms");
        System.out.println(String.format("Throughput: %.2f ops/sec\n", opsPerSec));
    }
}