package com.hurricache.client.cluster.stress;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.KeyHint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FastCacheContainerSegregatedStressTest {

    private final int THREAD_COUNT = 32;
    private final int OPERATIONS_PER_THREAD = 100_000;
    private final int PIPELINE_BATCH_SIZE = 100;
    private final int EXPECTED_TOTAL_OPS = THREAD_COUNT * OPERATIONS_PER_THREAD;
    private final int BATCH_TIMEOUT_SECONDS = 10;

    private static final byte[] ELEMENT_PAYLOAD
            = "container_payload_buffer_item_data_bytes".getBytes(StandardCharsets.UTF_8);
    private static final List<byte[]> INITIAL_ELEMENT_LIST = Collections.singletonList(ELEMENT_PAYLOAD);

    private String sessionGuidPrefix;
    private FastCacheAsyncSmartClient client;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws InterruptedException {
        // Уникальный UUID-префикс для всей сессии запуска
        sessionGuidPrefix = UUID.randomUUID().toString() + "-" + System.currentTimeMillis() + ":::";
        executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Подключение к реальному серверу fastcache
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(5);
            }
        }.setMode(Mode.MASTER_THAN_BACKUP);

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

    /**
     * Генерирует длинный уникальный ключ, содержащий UUID и иерархический суффикс
     */
    private String buildGuidKey(String type, int threadId, int opId) {
        return new StringBuilder(64)
                .append(sessionGuidPrefix)
                .append(type)
                .append("-th-")
                .append(threadId)
                .append("-op-")
                .append(opId)
                .append("-")
                .append(UUID.randomUUID()) // гарантирует длинный уникальный GUID для каждого ключа
                .toString();
    }

    private void printResults(String phaseName, long startTime, int successCount, int errorCount) {
        long durationMs = System.currentTimeMillis() - startTime;
        double opsPerSec = (successCount / (durationMs / 1000.0));
        System.out.printf("[%s] Ops: %d | Errors: %d | Time: %d ms | Throughput: %.2f ops/sec%n",
                          phaseName, successCount, errorCount, durationMs, opsPerSec);
    }

    // =========================================================================
    // TEST 1: ISOLATED LIST PIPELINE STRESS TEST
    // =========================================================================
    @Test
    void highConcurrencyListLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];
        String[][] keyStorage = new String[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Creation Sprint
        System.out.println("--- Starting List Creation Sprint ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        AtomicInteger createErrors = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String guidKey = buildGuidKey("list", threadId, j);
                        keyStorage[threadId][j] = guidKey;

                        pipeline.add(client.createList(guidKey.getBytes(StandardCharsets.UTF_8), INITIAL_ELEMENT_LIST));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processKeyHintBatch(pipeline, indexMapping, threadLocalStorage[threadId], createSuccess, createErrors);
                            pipeline.clear();
                            indexMapping.clear();
                        }
                    }
                } finally {
                    createLatch.countDown();
                }
            });
        }
        createLatch.await(10, TimeUnit.MINUTES);
        printResults("List Initialization", startTime, createSuccess.get(), createErrors.get());

        // 2. Stream Reading Sprint
        System.out.println("--- Starting List Stream Reading Sprint ---");
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger readSuccess = new AtomicInteger(0);
        AtomicInteger readErrors = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<List<byte[]>>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = keyStorage[threadId][j];
                        KeyHint hint = threadLocalStorage[threadId][j];
                        pipeline.add(client.streamList(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processReadBatch(pipeline, readSuccess, readErrors);
                            pipeline.clear();
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }
        readLatch.await(10, TimeUnit.MINUTES);
        printResults("List Streaming", startTime, readSuccess.get(), readErrors.get());

        Assertions.assertEquals(EXPECTED_TOTAL_OPS, createSuccess.get(), "List creation drops detected.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, readSuccess.get(), "List streaming read drops detected.");
    }

    // =========================================================================
    // TEST 2: ISOLATED QUEUE FIFO (ADD TO TAIL -> GET FROM HEAD) STRESS TEST
    // =========================================================================
    @Test
    void highConcurrencyQueueFifoLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];
        String[][] keyStorage = new String[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Creation Sprint (Initialize Empty Queues)
        System.out.println("--- Starting Queue Dynamic Initialization ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        AtomicInteger createErrors = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String guidKey = buildGuidKey("queue-fifo", threadId, j);
                        keyStorage[threadId][j] = guidKey;

                        pipeline.add(client.createQueue(guidKey.getBytes(StandardCharsets.UTF_8), Collections.emptyList()));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processKeyHintBatch(pipeline, indexMapping, threadLocalStorage[threadId], createSuccess, createErrors);
                            pipeline.clear();
                            indexMapping.clear();
                        }
                    }
                } finally {
                    createLatch.countDown();
                }
            });
        }
        createLatch.await(10, TimeUnit.MINUTES);
        printResults("Queue Empty Initialization", startTime, createSuccess.get(), createErrors.get());

        // 2. Add To Tail Sprint
        System.out.println("--- Starting Queue AddToTail Pipeline Sprint ---");
        CountDownLatch appendLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger appendSuccess = new AtomicInteger(0);
        AtomicInteger appendErrors = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = keyStorage[threadId][j];
                        KeyHint hint = threadLocalStorage[threadId][j];
                        pipeline.add(client.addElementToTail(key, hint, INITIAL_ELEMENT_LIST));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processBooleanBatch(pipeline, appendSuccess, appendErrors);
                            pipeline.clear();
                        }
                    }
                } finally {
                    appendLatch.countDown();
                }
            });
        }
        appendLatch.await(10, TimeUnit.MINUTES);
        printResults("Queue Tail Appends", startTime, appendSuccess.get(), appendErrors.get());

        // 3. Get and Remove from Head Sprint (FIFO Verification)
        System.out.println("--- Starting Queue GetFromHead Pipeline Eviction Sprint ---");
        CountDownLatch popLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger popSuccess = new AtomicInteger(0);
        AtomicInteger popErrors = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = keyStorage[threadId][j];
                        KeyHint hint = threadLocalStorage[threadId][j];
                        pipeline.add(client.getAndRemoveFront(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            processByteArrayBatch(pipeline, popSuccess, popErrors);
                            pipeline.clear();
                        }
                    }
                } finally {
                    popLatch.countDown();
                }
            });
        }
        popLatch.await(10, TimeUnit.MINUTES);
        printResults("Queue Head Pops (FIFO Verification)", startTime, popSuccess.get(), popErrors.get());

        Assertions.assertEquals(EXPECTED_TOTAL_OPS, createSuccess.get(), "Queue allocation drops detected.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, appendSuccess.get(), "Inbound pipeline data mutations dropped.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, popSuccess.get(), "FIFO execution payload data corrupted.");
    }

    // =========================================================================
    // HELPER PIPELINE PROCESSORS (ALL-OF PATTERN)
    // =========================================================================

    private void processKeyHintBatch(List<CompletableFuture<KeyHint>> pipeline,
                                     List<Integer> indexMapping,
                                     KeyHint[] threadStorage,
                                     AtomicInteger successCounter,
                                     AtomicInteger errorCounter) {
        try {
            CompletableFuture<Void> allOf = CompletableFuture.allOf(pipeline.toArray(new CompletableFuture[0]));
            allOf.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (int k = 0; k < pipeline.size(); k++) {
                KeyHint hint = pipeline.get(k).getNow(null);
                if (hint != null) {
                    if (threadStorage != null) {
                        threadStorage[indexMapping.get(k)] = hint;
                    }
                    successCounter.incrementAndGet();
                } else {
                    errorCounter.incrementAndGet();
                }
            }
        } catch (Exception e) {
            errorCounter.addAndGet(pipeline.size());
        }
    }

    private void processReadBatch(List<CompletableFuture<List<byte[]>>> pipeline,
                                  AtomicInteger successCounter,
                                  AtomicInteger errorCounter) {
        try {
            CompletableFuture<Void> allOf = CompletableFuture.allOf(pipeline.toArray(new CompletableFuture[0]));
            allOf.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (CompletableFuture<List<byte[]>> future : pipeline) {
                List<byte[]> res = future.getNow(null);
                if (res != null && !res.isEmpty()) {
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

    private void processByteArrayBatch(List<CompletableFuture<byte[]>> pipeline,
                                       AtomicInteger successCounter,
                                       AtomicInteger errorCounter) {
        try {
            CompletableFuture<Void> allOf = CompletableFuture.allOf(pipeline.toArray(new CompletableFuture[0]));
            allOf.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (CompletableFuture<byte[]> future : pipeline) {
                byte[] data = future.getNow(null);
                if (data != null && data.length == ELEMENT_PAYLOAD.length) {
                    successCounter.incrementAndGet();
                } else {
                    errorCounter.incrementAndGet();
                }
            }
        } catch (Exception e) {
            errorCounter.addAndGet(pipeline.size());
        }
    }
}