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
import java.util.concurrent.atomic.LongAdder;

public class FastCacheRawStressTest {

    private final String prefix = UUID.randomUUID() + "-" + System.currentTimeMillis() + ":::";
    private static final int THREAD_COUNT = 16;
    private static final int OPERATIONS_PER_THREAD = 200_000;
    private static final int PIPELINE_BATCH_SIZE = 256;
    private static final int EXPECTED_TOTAL_OPS = THREAD_COUNT * OPERATIONS_PER_THREAD;
    private static final int BATCH_TIMEOUT_SECONDS = 10;

    private static final byte[] PREALLOCATED_VALUE = "value_data_payload_placeholder_for_stress_testing".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREALLOCATED_UPDATE = "value_data_payload_placeholder_for_stress_testing_updated".getBytes(StandardCharsets.UTF_8);

    private FastCacheAsyncSmartClient client;
    private ExecutorService executor;

    // Кэш сгенерированных ключей: [threadId][opId]
    private String[][] pregeneratedKeys;

    @BeforeEach
    void setUp() throws InterruptedException {
        executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // 1. Предварительная генерация всех ключей до старта бенчмарка
        System.out.println("Pre-generating " + EXPECTED_TOTAL_OPS + " keys to eliminate String allocations during test...");
        pregeneratedKeys = new String[THREAD_COUNT][OPERATIONS_PER_THREAD];
        for (int i = 0; i < THREAD_COUNT; i++) {
            for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                pregeneratedKeys[i][j] = prefix + "stress:" + i + ":" + j;
            }
        }

        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(5);
            }
        };
        client.setMode(Mode.LB_SMART);

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
        pregeneratedKeys = null;
    }

    @Test
    void highConcurrencyCreateLoadTest() throws InterruptedException {
        KeyHintData[][] threadLocalStorage = new KeyHintData[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // ==========================================
        // 1. PIPELINED WRITE SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Writes...");
        CountDownLatch writeLatch = new CountDownLatch(THREAD_COUNT);
        LongAdder writeSuccessCount = new LongAdder();
        LongAdder writeErrorCount = new LongAdder();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHintData>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                CompletableFuture<?>[] batchArray = new CompletableFuture[PIPELINE_BATCH_SIZE];

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = pregeneratedKeys[threadId][j];

                        pipeline.add(client.createKeyValue(key, PREALLOCATED_VALUE));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            int startIdx = j - pipeline.size() + 1;
                            processWriteBatch(pipeline, batchArray, threadLocalStorage[threadId], startIdx, writeSuccessCount, writeErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    writeLatch.countDown();
                }
            });
        }

        writeLatch.await(10, TimeUnit.MINUTES);
        printResults("Write", startTime, writeSuccessCount.sum(), writeErrorCount.sum());

        // ==========================================
        // 2. PIPELINED READ SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Reads...");
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);
        LongAdder readSuccessCount = new LongAdder();
        LongAdder readErrorCount = new LongAdder();

        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                CompletableFuture<?>[] batchArray = new CompletableFuture[PIPELINE_BATCH_SIZE];

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = pregeneratedKeys[threadId][j];
                        KeyHintData hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.getValue(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processReadBatch(pipeline, batchArray, readSuccessCount, readErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }

        readLatch.await(10, TimeUnit.MINUTES);
        printResults("Read", startTime, readSuccessCount.sum(), readErrorCount.sum());

        // ==========================================
        // 3. PIPELINED UPDATE SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Updates...");
        CountDownLatch updateLatch = new CountDownLatch(THREAD_COUNT);
        LongAdder updateSuccessCount = new LongAdder();
        LongAdder updateErrorCount = new LongAdder();

        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                CompletableFuture<?>[] batchArray = new CompletableFuture[PIPELINE_BATCH_SIZE];

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = pregeneratedKeys[threadId][j];
                        KeyHintData hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.updateKeyValue(key, hint, PREALLOCATED_UPDATE));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processReadBatch(pipeline, batchArray, updateSuccessCount, updateErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    updateLatch.countDown();
                }
            });
        }

        updateLatch.await(10, TimeUnit.MINUTES);
        printResults("Update", startTime, updateSuccessCount.sum(), updateErrorCount.sum());

        // ==========================================
        // 4. PIPELINED DELETE SPRINT
        // ==========================================
        System.out.println("Executing Pipelined Deletes...");
        CountDownLatch deleteLatch = new CountDownLatch(THREAD_COUNT);
        LongAdder deleteSuccessCount = new LongAdder();
        LongAdder deleteErrorCount = new LongAdder();

        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                CompletableFuture<?>[] batchArray = new CompletableFuture[PIPELINE_BATCH_SIZE];

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = pregeneratedKeys[threadId][j];
                        KeyHintData hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.remove(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processBooleanBatch(pipeline, batchArray, deleteSuccessCount, deleteErrorCount);
                            pipeline.clear();
                        }
                    }
                } finally {
                    deleteLatch.countDown();
                }
            });
        }

        deleteLatch.await(10, TimeUnit.MINUTES);
        printResults("Delete", startTime, deleteSuccessCount.sum(), deleteErrorCount.sum());

        // Validations
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, writeSuccessCount.sum(), "Write validation mismatch");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, readSuccessCount.sum(), "Read validation mismatch");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, updateSuccessCount.sum(), "Update validation mismatch");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, deleteSuccessCount.sum(), "Delete validation mismatch");
    }

    // =========================================================================
    // OPTIMIZED BATCH PROCESSORS
    // =========================================================================

    private void processWriteBatch(List<CompletableFuture<KeyHintData>> pipeline,
                                   CompletableFuture<?>[] batchArray,
                                   KeyHintData[] threadStorage,
                                   int startIdx,
                                   LongAdder successCounter,
                                   LongAdder errorCounter) {
        int size = pipeline.size();
        try {
            for (int i = 0; i < size; i++) {
                batchArray[i] = pipeline.get(i);
            }

            // Быстрое ожидание завершения без пересоздания массивов
            CompletableFuture.allOf(size == PIPELINE_BATCH_SIZE ? batchArray : pipeline.toArray(new CompletableFuture[0]))
                    .get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (int k = 0; k < size; k++) {
                KeyHintData hint = pipeline.get(k).getNow(null);
                if (hint != null) {
                    threadStorage[startIdx + k] = hint;
                    successCounter.increment();
                } else {
                    errorCounter.increment();
                }
            }
        } catch (Exception e) {
            errorCounter.add(size);
        }
    }

    private void processReadBatch(List<CompletableFuture<byte[]>> pipeline,
                                  CompletableFuture<?>[] batchArray,
                                  LongAdder successCounter,
                                  LongAdder errorCounter) {
        int size = pipeline.size();
        try {
            for (int i = 0; i < size; i++) {
                batchArray[i] = pipeline.get(i);
            }

            CompletableFuture.allOf(size == PIPELINE_BATCH_SIZE ? batchArray : pipeline.toArray(new CompletableFuture[0]))
                    .get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (int k = 0; k < size; k++) {
                byte[] data = pipeline.get(k).getNow(null);
                if (data != null) {
                    successCounter.increment();
                } else {
                    errorCounter.increment();
                }
            }
        } catch (Exception e) {
            errorCounter.add(size);
        }
    }

    private void processBooleanBatch(List<CompletableFuture<Boolean>> pipeline,
                                     CompletableFuture<?>[] batchArray,
                                     LongAdder successCounter,
                                     LongAdder errorCounter) {
        int size = pipeline.size();
        try {
            for (int i = 0; i < size; i++) {
                batchArray[i] = pipeline.get(i);
            }

            CompletableFuture.allOf(size == PIPELINE_BATCH_SIZE ? batchArray : pipeline.toArray(new CompletableFuture[0]))
                    .get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (int k = 0; k < size; k++) {
                if (Boolean.TRUE.equals(pipeline.get(k).getNow(false))) {
                    successCounter.increment();
                } else {
                    errorCounter.increment();
                }
            }
        } catch (Exception e) {
            errorCounter.add(size);
        }
    }

    private void printResults(String operationalPhase, long startTime, long successCount, long errorCount) {
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