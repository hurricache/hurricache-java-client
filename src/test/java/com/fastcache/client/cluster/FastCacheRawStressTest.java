package com.fastcache.client.cluster;

import com.fastcache.TestBase;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
import io.grpc.inprocess.InProcessServerBuilder;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FastCacheRawStressTest {

    private final static String prefix = UUID.randomUUID()+"-"+System.currentTimeMillis()+":::";
    private final int THREAD_COUNT = 32; // Optimized for i9
    private final int OPERATIONS_PER_THREAD = 100000;
    private final int PIPELINE_BATCH_SIZE = 100; // Pipeline requests to saturate C++ engine
    private String serverName;

    // Pre-allocated structural byte payloads to completely eliminate JVM GC noise during the sprint
    private static final byte[] PREALLOCATED_VALUE = "value_data_payload_placeholder_for_stress_testing".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREALLOCATED_UPDATE = "value_data_payload_placeholder_for_stress_testing_updated".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void init() throws IOException {
        serverName = "stress-server-" + UUID.randomUUID();
        InProcessServerBuilder.forName(serverName).addService(new TestBase.MockFastCacheService()).build().start();
    }

    @Test
    void highConcurrencyCreateLoadTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        FastCacheAsyncSmartClient client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)){

            public Duration getDefaultTtl(){
                return Duration.ofMinutes(15);
            }
        };
        client.setMode(FastCacheAsyncSmartClient.Mode.LB_SMART);

        while (!client.getReadyFlag()) {
            Thread.sleep(100);
        }

        // Partition storage arrays per thread to eliminate concurrent lock contention
        KeyHint[][] threadLocalStorage = new KeyHint[THREAD_COUNT][OPERATIONS_PER_THREAD];

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
                List<CompletableFuture<KeyHint>> pipeline = new ArrayList<>(PIPELINE_BATCH_SIZE);
                List<Integer> indexMapping = new ArrayList<>(PIPELINE_BATCH_SIZE);

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = getTestValue() + threadId + ":" + j;

                        // Fire asynchronously without blocking
                        CompletableFuture<KeyHint> future = client.createKeyValue(key, PREALLOCATED_VALUE);
                        pipeline.add(future);
                        indexMapping.add(j);

                        // Once the pipeline batch is full, flush it and resolve elements concurrently
                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (int k = 0; k < pipeline.size(); k++) {
                                try {
                                    KeyHint hint = pipeline.get(k).get(1000, TimeUnit.MILLISECONDS);
                                    threadLocalStorage[threadId][indexMapping.get(k)] = hint;
                                    writeSuccessCount.incrementAndGet();
                                } catch (Exception e) {
                                    System.out.println("Error: "+e.getLocalizedMessage());
                                    writeErrorCount.incrementAndGet();
                                }
                            }
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
                        String key = getTestValue() + threadId + ":" + j;
                        KeyHint hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.getValue(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<byte[]> future : pipeline) {
                                try {
                                    byte[] res = future.get(1000, TimeUnit.MILLISECONDS);
                                    if (res != null) readSuccessCount.incrementAndGet();
                                    else readErrorCount.incrementAndGet();
                                } catch (Exception e) {
                                    readErrorCount.incrementAndGet();
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
                        String key = getTestValue() + threadId + ":" + j;
                        KeyHint hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.updateKeyValue(key, hint, PREALLOCATED_UPDATE));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<byte[]> future : pipeline) {
                                try {
                                    future.get(1000, TimeUnit.MILLISECONDS);
                                    updateSuccessCount.incrementAndGet();
                                } catch (Exception e) {
                                    updateErrorCount.incrementAndGet();
                                }
                            }
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
                        String key = getTestValue() + threadId + ":" + j;
                        KeyHint hint = threadLocalStorage[threadId][j];

                        pipeline.add(client.remove(key, hint));

                        if (pipeline.size() == PIPELINE_BATCH_SIZE || j == OPERATIONS_PER_THREAD - 1) {
                            for (CompletableFuture<Boolean> future : pipeline) {
                                try {
                                    if (future.get(1000, TimeUnit.MILLISECONDS)) deleteSuccessCount.incrementAndGet();
                                    else deleteErrorCount.incrementAndGet();
                                } catch (Exception e) {
                                    deleteErrorCount.incrementAndGet();
                                }
                            }
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

        // Tear down
        client.shutdown();
        executor.shutdown();

        // Standard assertions to enforce integrity
        int expectedTotal = THREAD_COUNT * OPERATIONS_PER_THREAD;
        Assertions.assertEquals(expectedTotal, writeSuccessCount.get(), "Write validation mismatch");
        Assertions.assertEquals(expectedTotal, readSuccessCount.get(), "Read validation mismatch");
        Assertions.assertEquals(expectedTotal, updateSuccessCount.get(), "Update validation mismatch");
        Assertions.assertEquals(expectedTotal, deleteSuccessCount.get(), "Delete validation mismatch");
    }

    private static @NonNull String getTestValue() {
        return prefix +"stress:";
    }

    private void printResults(String operationalPhase, long startTime, int successCount, int errorCount) {
        long duration = System.currentTimeMillis() - startTime;
        double opsPerSec = (double) (successCount + errorCount) / (duration / 1000.0);
        System.out.println(String.format("--- Stress Test %s Results ---", operationalPhase));
        System.out.println("Total Operations: " + (successCount + errorCount));
        System.out.println("Successes: " + successCount);
        System.out.println("Errors: " + errorCount);
        System.out.println("Duration: " + duration + " ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec\n");
    }
}