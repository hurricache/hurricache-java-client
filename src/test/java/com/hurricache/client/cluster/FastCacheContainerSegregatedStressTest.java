package com.hurricache.client.cluster;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.grpc.KeyHint;
import org.jspecify.annotations.NonNull;
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

    private String prefix;
    private final int THREAD_COUNT = 32;
    private final int OPERATIONS_PER_THREAD = 100000;
    private final int PIPELINE_BATCH_SIZE = 100;
    private final int EXPECTED_TOTAL_OPS = THREAD_COUNT * OPERATIONS_PER_THREAD;

    private static final byte[] ELEMENT_PAYLOAD
            = "container_payload_buffer_item_data_bytes".getBytes(StandardCharsets.UTF_8);
    private static final List<byte[]> INITIAL_ELEMENT_LIST = Collections.singletonList(ELEMENT_PAYLOAD);

    private FastCacheAsyncSmartClient client;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws InterruptedException {
        prefix = UUID.randomUUID() + "-" + System.currentTimeMillis() + ":::";
        executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Connect to the real local server instance
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(5);
            }
        }.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);

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

    // =========================================================================
    // TEST 1: ISOLATED LIST PIPELINE STRESS TEST
    // =========================================================================
    @Test
    void highConcurrencyListLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Creation Sprint
        System.out.println("--- Starting List Creation Sprint ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        byte[] keyBytes = getTestKey("list", threadId, j).getBytes(StandardCharsets.UTF_8);
                        pipeline.add(client.createList(keyBytes, INITIAL_ELEMENT_LIST));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (int k = 0; k < pipeline.size(); k++) {
                                try {
                                    threadLocalStorage[threadId][indexMapping.get(k)] = pipeline.get(k)
                                            .get(1000, TimeUnit.MILLISECONDS);
                                    createSuccess.incrementAndGet();
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
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
        printResults("List Initialization", startTime, createSuccess.get());

        // 2. Stream Reading Sprint
        System.out.println("--- Starting List Stream Reading Sprint ---");
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger readSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<List<byte[]>>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("list", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];
                        pipeline.add(client.streamList(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<List<byte[]>> future : pipeline) {
                                try {
                                    List<byte[]> res = future.get(1000, TimeUnit.MILLISECONDS);
                                    if (res != null && !res.isEmpty()) {
                                        readSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }
        readLatch.await(10, TimeUnit.MINUTES);
        printResults("List Streaming", startTime, readSuccess.get());

        // Assertions Validation
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, createSuccess.get(), "List creation drops detected.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, readSuccess.get(), "List streaming read drops detected.");
    }

    // =========================================================================
    // TEST 2: ISOLATED VECTOR PIPELINE STRESS TEST
    // =========================================================================
    @Test
    void highConcurrencyVectorLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Creation Sprint
        System.out.println("--- Starting Vector Creation Sprint ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        byte[] keyBytes = getTestKey("vector", threadId, j).getBytes(StandardCharsets.UTF_8);
                        pipeline.add(client.createVector(keyBytes,
                                INITIAL_ELEMENT_LIST,
                                Duration.ZERO,
                                1,
                                Duration.ofSeconds(2)));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (int k = 0; k < pipeline.size(); k++) {
                                try {
                                    threadLocalStorage[threadId][indexMapping.get(k)] = pipeline.get(k)
                                            .get(1000, TimeUnit.MILLISECONDS);
                                    createSuccess.incrementAndGet();
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
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
        printResults("Vector Initialization", startTime, createSuccess.get());

        // 2. Stream Reading Sprint
        System.out.println("--- Starting Vector Stream Reading Sprint ---");
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger readSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<List<byte[]>>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("vector", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];
                        pipeline.add(client.streamVector(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<List<byte[]>> future : pipeline) {
                                try {
                                    List<byte[]> res = future.get(1000, TimeUnit.MILLISECONDS);
                                    if (res != null && !res.isEmpty()) {
                                        readSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }
        readLatch.await(10, TimeUnit.MINUTES);
        printResults("Vector Streaming", startTime, readSuccess.get());

        // Assertions Validation
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, createSuccess.get(), "Vector creation drops detected.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, readSuccess.get(), "Vector streaming read drops detected.");
    }

    // =========================================================================
    // TEST 4: ISOLATED QUEUE FIFO (ADD TO TAIL -> GET FROM HEAD) STRESS TEST
    // =========================================================================
    @Test
    void highConcurrencyQueueFifoLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Creation Sprint (Initialize Empty Queues)
        System.out.println("--- Starting Queue Dynamic Initialization ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        byte[] keyBytes = getTestKey("queue-fifo", threadId, j).getBytes(StandardCharsets.UTF_8);
                        // Initialize empty queues to populate via tail-appends later
                        pipeline.add(client.createQueue(keyBytes,
                                Collections.emptyList()));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (int k = 0; k < pipeline.size(); k++) {
                                try {
                                    threadLocalStorage[threadId][indexMapping.get(k)] = pipeline.get(k)
                                            .get(1000, TimeUnit.MILLISECONDS);
                                    createSuccess.incrementAndGet();
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
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
        printResults("Queue Empty Initialization", startTime, createSuccess.get());

        // 2. Add To Tail Sprint
        System.out.println("--- Starting Queue AddToTail Pipeline Sprint ---");
        CountDownLatch appendLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger appendSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("queue-fifo", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        // Blast element to the tail end of the designated queue instance
                        pipeline.add(client.addElementToTail(key, hint, INITIAL_ELEMENT_LIST));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<Boolean> future : pipeline) {
                                try {
                                    if (Boolean.TRUE.equals(future.get(1000, TimeUnit.MILLISECONDS))) {
                                        appendSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    appendLatch.countDown();
                }
            });
        }
        appendLatch.await(10, TimeUnit.MINUTES);
        printResults("Queue Tail Appends", startTime, appendSuccess.get());

        // 3. Get and Remove from Head Sprint (FIFO Verification)
        System.out.println("--- Starting Queue GetFromHead Pipeline Eviction Sprint ---");
        CountDownLatch popLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger popSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("queue-fifo", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        // Atomically pull and evict from the front head of the structural sequence
                        pipeline.add(client.getAndRemoveFront(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<byte[]> future : pipeline) {
                                try {
                                    byte[] evictedData = future.get(1000, TimeUnit.MILLISECONDS);
                                    // Assert data integrity matching pre-allocated write array constants
                                    if (evictedData != null && evictedData.length == ELEMENT_PAYLOAD.length) {
                                        popSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    popLatch.countDown();
                }
            });
        }
        popLatch.await(10, TimeUnit.MINUTES);
        printResults("Queue Head Pops (FIFO Verification)", startTime, popSuccess.get());

        // Assertions Validation
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, createSuccess.get(), "Queue structure allocation leaks detected.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, appendSuccess.get(), "Inbound pipeline data mutations dropped.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, popSuccess.get(), "FIFO execution state/payload data corrupted.");
    }

    // =========================================================================
    // TEST 5: ISOLATED LIST FIFO (ADD TO TAIL -> GET FROM INDEX 0) STRESS TEST
    // =========================================================================
    @Test
    void highConcurrencyListFifoLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Initialization Sprint
        System.out.println("--- Starting List Empty Initialization ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        byte[] keyBytes = getTestKey("list-fifo", threadId, j).getBytes(StandardCharsets.UTF_8);
                        // Allocate empty root structures
                        pipeline.add(client.createList(keyBytes,
                                Collections.emptyList()));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (int k = 0; k < pipeline.size(); k++) {
                                try {
                                    threadLocalStorage[threadId][indexMapping.get(k)] = pipeline.get(k)
                                            .get(1000, TimeUnit.MILLISECONDS);
                                    createSuccess.incrementAndGet();
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
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
        printResults("List Structural Allocations", startTime, createSuccess.get());

        // 2. Enqueue Sprint (Add to Tail)
        System.out.println("--- Starting List Enqueue (AddToTail) Pipeline Sprint ---");
        CountDownLatch enqueueLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger enqueueSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("list-fifo", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.addElementToTail(key, hint, INITIAL_ELEMENT_LIST));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<Boolean> future : pipeline) {
                                try {
                                    if (Boolean.TRUE.equals(future.get(1000, TimeUnit.MILLISECONDS))) {
                                        enqueueSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    enqueueLatch.countDown();
                }
            });
        }
        enqueueLatch.await(10, TimeUnit.MINUTES);
        printResults("List Enqueue via Tail Appends", startTime, enqueueSuccess.get());

        // 3. Dequeue Sprint (Get & Remove from Position 0)
        System.out.println("--- Starting List Dequeue (GetFromPosition 0) Pipeline Sprint ---");
        CountDownLatch dequeueLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger dequeueSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("list-fifo", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        // Pass pos = 0 to achieve strict FIFO consumption behavior
                        pipeline.add(client.getAndRemoveElementAtPosition(client.serializeKey(key), hint, 0));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<byte[]> future : pipeline) {
                                try {
                                    byte[] data = future.get(1000, TimeUnit.MILLISECONDS);
                                    if (data != null && data.length == ELEMENT_PAYLOAD.length) {
                                        dequeueSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    dequeueLatch.countDown();
                }
            });
        }
        dequeueLatch.await(10, TimeUnit.MINUTES);
        printResults("List Dequeue via Index 0 Extraction", startTime, dequeueSuccess.get());

        // Hard Core Assertions
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, createSuccess.get(), "List allocation dropped data references.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS,
                enqueueSuccess.get(),
                "Inbound FIFO mutations dropped under pressure.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS,
                dequeueSuccess.get(),
                "Index zero eviction data tracking corrupted.");
    }

    // =========================================================================
    // TEST 6: ISOLATED VECTOR RANDOM ACCESS (INDEXED READ, WRITE & DELETION)
    // =========================================================================
    @Test
    void highConcurrencyVectorRandomAccessLoadTest() throws InterruptedException {
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];

        // 1. Structural Initialization Sprint (Pre-populate with initial elements)
        System.out.println("--- Starting Vector Initialization ---");
        CountDownLatch createLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger createSuccess = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        byte[] keyBytes = getTestKey("vector-rand", threadId, j).getBytes(StandardCharsets.UTF_8);
                        // Initialize with an initial payload to guarantee slot 0 is occupied
                        pipeline.add(client.createVector(keyBytes,
                                INITIAL_ELEMENT_LIST));
                        indexMapping.add(j);

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (int k = 0; k < pipeline.size(); k++) {
                                try {
                                    threadLocalStorage[threadId][indexMapping.get(k)] = pipeline.get(k)
                                            .get(1000, TimeUnit.MILLISECONDS);
                                    createSuccess.incrementAndGet();
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
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
        printResults("Vector Structural Allocations", startTime, createSuccess.get());

        // 2. Random Position Insertion Sprint (Injecting explicitly into Position 1)
        System.out.println("--- Starting Vector Random Access Insertion (Position 1) ---");
        CountDownLatch insertLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger insertSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("vector-rand", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        // Explicitly targeting random-access index position 1
                        pipeline.add(client.addElementToPosition(key, hint, INITIAL_ELEMENT_LIST, 1));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<Boolean> future : pipeline) {
                                try {
                                    if (Boolean.TRUE.equals(future.get(1000, TimeUnit.MILLISECONDS))) {
                                        insertSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    insertLatch.countDown();
                }
            });
        }
        insertLatch.await(10, TimeUnit.MINUTES);
        printResults("Vector Random Index Insertions", startTime, insertSuccess.get());

        // 3. Random Access Point Read Sprint (Validating content from Position 1)
        System.out.println("--- Starting Vector Random Access Reads (From Position 1) ---");
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger readSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<byte[]>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("vector-rand", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        // Direct lookup targeting index position 1
                        pipeline.add(client.getElementAtPosition(key, hint, 1));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<byte[]> future : pipeline) {
                                try {
                                    byte[] data = future.get(1000, TimeUnit.MILLISECONDS);
                                    if (data != null && data.length == ELEMENT_PAYLOAD.length) {
                                        readSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }
        readLatch.await(10, TimeUnit.MINUTES);
        printResults("Vector Random Index Lookups", startTime, readSuccess.get());

        // 4. Random Access Removal Sprint (Evicting element at Position 1)
        System.out.println("--- Starting Vector Random Access Removals (From Position 1) ---");
        CountDownLatch removeLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger removeSuccess = new AtomicInteger(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                List<CompletableFuture<Boolean>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestKey("vector-rand", threadId, j);
                        KeyHint hint = threadLocalStorage[threadId][j];

                        // Random eviction directly targeting index position 1
                        pipeline.add(client.removeElementAtPosition(key, hint, 1));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<Boolean> future : pipeline) {
                                try {
                                    if (Boolean.TRUE.equals(future.get(1000, TimeUnit.MILLISECONDS))) {
                                        removeSuccess.incrementAndGet();
                                    }
                                } catch (Exception ignored) {
                                    System.out.println("Operation failed: " + ignored.getMessage());
                                }
                            }
                            pipeline.clear();
                        }
                    }
                } finally {
                    removeLatch.countDown();
                }
            });
        }
        removeLatch.await(10, TimeUnit.MINUTES);
        printResults("Vector Random Index Removals", startTime, removeSuccess.get());

        // Hard Boundary Validation Assertions
        Assertions.assertEquals(EXPECTED_TOTAL_OPS,
                createSuccess.get(),
                "Vector base instance creation failures encountered.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, insertSuccess.get(), "Random-access writes at index 1 dropped.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS,
                readSuccess.get(),
                "Random-access reads at index 1 mismatched or corrupted.");
        Assertions.assertEquals(EXPECTED_TOTAL_OPS, removeSuccess.get(), "Random-access deletions at index 1 failed.");
    }

    private @NonNull String getTestKey(String type, int threadId, int elementIndex) {
        return prefix + type + ":" + threadId + ":" + elementIndex;
    }

    private void printResults(String operationalPhase, long startTime, int successCount) {
        long duration = System.currentTimeMillis() - startTime;
        int errorCount = EXPECTED_TOTAL_OPS - successCount;
        double opsPerSec = (double) (EXPECTED_TOTAL_OPS) / (duration / 1000.0);
        System.out.println(String.format("--- %s Load Performance Results ---", operationalPhase));
        System.out.println("Total Intended Operations: " + EXPECTED_TOTAL_OPS);
        System.out.println("Successes: " + successCount);
        System.out.println("Errors/Drops: " + errorCount);
        System.out.println("Duration: " + duration + " ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec\n");
    }
}