package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

public class ExtendedHurriCacheStressTest extends TestBaseCluster {

    private static final int THREAD_COUNT = 16;
    private static final int OPS_PER_THREAD = 5000;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private byte[] bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 1. STRESS: ATOMIC BITWISE & CAS OPERATIONS UNDER HIGH CONTENTION
    // =========================================================================
    @Test
    void testAtomicCasAndBitwiseHighContention() throws Exception {
        String testKey = "stress_cas_bitwise_" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 0L).get();
        assertNotNull(hint);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger successfulCasOps = new AtomicInteger(0);
        LongAdder totalCasAttempts = new LongAdder();
        LongAdder bitwiseOpsCount = new LongAdder();

        long startTime = System.nanoTime();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < OPS_PER_THREAD; j++) {
                        if (j % 3 == 0) {
                            client.atomicOr(bytes(testKey), hint, 1L << (threadId % 63)).get();
                            client.atomicXor(bytes(testKey), hint, 1L << (threadId % 63)).get();
                            bitwiseOpsCount.add(2); // 2 битовые операции
                        } else {
                            boolean casSuccess = false;
                            while (!casSuccess) {
                                totalCasAttempts.increment();
                                long currentVal = client.atomicLoad(bytes(testKey), hint).get();
                                AtomicCasRes res = client.atomicCompareAndSet(
                                        bytes(testKey), hint, currentVal, currentVal + 1
                                ).get();

                                if (res != null && res.getResult()) {
                                    successfulCasOps.incrementAndGet();
                                    casSuccess = true;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = finishLatch.await(60, TimeUnit.SECONDS);
        long durationNs = System.nanoTime() - startTime;
        executor.shutdown();

        assertTrue(completed, "Тест завис при проверке CAS/Bitwise операций");

        double durationSec = durationNs / 1_000_000_000.0;
        long totalSuccessfulOps = successfulCasOps.get() + bitwiseOpsCount.sum();
        long totalNetworkRequests = totalCasAttempts.sum() * 2 + bitwiseOpsCount.sum(); // Load + CAS + Bitwise

        double throughputRps = totalSuccessfulOps / durationSec;
        double networkRps = totalNetworkRequests / durationSec;

        System.out.println("----------------------------------------------------------------");
        System.out.printf("[CAS/BITWISE STRESS] Executed in: %.2f sec%n", durationSec);
        System.out.printf("  - Successful Logical Ops: %d (%.2f ops/sec)%n", totalSuccessfulOps, throughputRps);
        System.out.printf("  - Total Network Requests: %d (%.2f req/sec)%n", totalNetworkRequests, networkRps);
        System.out.printf("  - CAS Retries (Conflicts): %d%n", totalCasAttempts.sum() - successfulCasOps.get());
        System.out.println("----------------------------------------------------------------");
    }

    // =========================================================================
    // 2. STRESS: ORDERED SET WITH WEIGHTS (CONTENDED RANGE & ADD)
    // =========================================================================
    // =========================================================================
    // 2. STRESS: ORDERED SET WITH WEIGHTS (CONTENDED ADD & POSITION GET)
    // =========================================================================
    @Test
    void testConcurrentOrderedSetWeightOperations() throws Exception {
        String setKey = "stress_ordered_set_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedSet(setKey, null).get();
        assertNotNull(hint);
        Thread.sleep(150);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(THREAD_COUNT);

        LongAdder addedElements = new LongAdder();
        LongAdder positionReads = new LongAdder();
        LongAdder notFoundErrors = new LongAdder();

        long startTime = System.nanoTime();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();

                    for (int j = 0; j < OPS_PER_THREAD; j++) {
                        long weight = random.nextLong(0, 10_000);
                        byte[] valBytes = ("val_" + threadId + "_" + j).getBytes(StandardCharsets.UTF_8);
                        Payload payload = Payload.of(valBytes);
                        OrderedPayload item = OrderedPayload.of(payload.getValue(), weight);

                        // 1. Вставка элемента с весом
                        client.addElementWithWeight(bytes(setKey), hint, List.of(item)).get();
                        addedElements.increment();

                        // 2. Точечное чтение элемента по рангу/позиции
                        // В высококонкурентной среде точный размер сета может "плавать",
                        // поэтому запрашиваем случайную позицию с запасом.
                        int currentSizeEst = (int) addedElements.sum();
                        int targetPos = currentSizeEst > 0 ? random.nextInt(0, currentSizeEst) : 0;

                        try {
                            Payload retrieved = client.getElementAtPosition(bytes(setKey), hint, targetPos).get();
                            if (retrieved != null) {
                                positionReads.increment();
                            }
                        } catch (ExecutionException e) {
                            // Перехватываем gRPC StatusRuntimeException
                            if (e.getCause() instanceof io.grpc.StatusRuntimeException) {
                                io.grpc.StatusRuntimeException grpcException = (io.grpc.StatusRuntimeException) e.getCause();
                                if (grpcException.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                                    // Это ожидаемое поведение при гонках (запросили позицию, которой еще/уже нет)
                                    notFoundErrors.increment();
                                } else {
                                    // Если ошибка другая (например, DEADLINE_EXCEEDED), пробрасываем дальше
                                    throw e;
                                }
                            } else {
                                throw e;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = finishLatch.await(60, TimeUnit.SECONDS);
        long durationNs = System.nanoTime() - startTime;
        executor.shutdown();

        assertTrue(completed, "OrderedSet стресс-тест превысил таймаут");

        double durationSec = durationNs / 1_000_000_000.0;
        long totalOps = addedElements.sum() + positionReads.sum() + notFoundErrors.sum();
        double throughputRps = totalOps / durationSec;

        System.out.println("----------------------------------------------------------------");
        System.out.printf("[ORDERED SET STRESS] Executed in: %.2f sec%n", durationSec);
        System.out.printf("  - Total Operations: %d (Adds: %d, Position Reads: %d)%n",
                          totalOps, addedElements.sum(), positionReads.sum());
        System.out.printf("  - NOT_FOUND Errors (race conditions): %d%n", notFoundErrors.sum());
        System.out.printf("  - Throughput: %.2f req/sec (RPS)%n", throughputRps);
        System.out.println("----------------------------------------------------------------");
    }

    // =========================================================================
    // 3. STRESS: CONCURRENT HASH MAP MUTATIONS AND STREAMING
    // =========================================================================
    // =========================================================================
    // 3. STRESS: CONCURRENT HASH MAP MUTATIONS AND STREAMING
    // =========================================================================
    @Test
    void testConcurrentHashMapOperations() throws Exception {
        String mapKey = "stress_hash_map_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(mapKey, null).get();
        assertNotNull(hint);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger mapBatchWrites = new AtomicInteger(0);
        AtomicInteger mapReads = new AtomicInteger(0);

        long startTime = System.nanoTime();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < OPS_PER_THREAD; j++) {
                        byte[] kBytes = ("key_" + threadId + "_" + j).getBytes(StandardCharsets.UTF_8);
                        byte[] vBytes = ("val_" + threadId + "_" + j).getBytes(StandardCharsets.UTF_8);

                        // Батч-добавление элемента в HashMap через предназначенный метод addElementHashMap
                        List<Payload> containerKeys = List.of(Payload.of(kBytes));
                        List<Payload> containerValues = List.of(Payload.of(vBytes));

                        client.addElementHashMap(
                                bytes(mapKey),
                                hint,
                                containerKeys,
                                containerValues,
                                client.getDefaultClientId(),
                                DEFAULT_TIMEOUT
                        ).get();
                        mapBatchWrites.incrementAndGet();

                        // Точечное получение значения элемента
                        byte[] retrieved = client.getContainerValue(bytes(mapKey), hint, kBytes).get();
                        assertNotNull(retrieved);
                        mapReads.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = finishLatch.await(60, TimeUnit.SECONDS);
        long durationNs = System.nanoTime() - startTime;
        executor.shutdown();

        assertTrue(completed, "HashMap стресс-тест превысил таймаут");

        double durationSec = durationNs / 1_000_000_000.0;
        long totalOps = mapBatchWrites.get() + mapReads.get();
        double throughputRps = totalOps / durationSec;

        System.out.println("----------------------------------------------------------------");
        System.out.printf("[HASH MAP STRESS] Executed in: %.2f sec%n", durationSec);
        System.out.printf("  - Total Operations: %d (Batch Writes: %d, Reads: %d)%n", totalOps, mapBatchWrites.get(), mapReads.get());
        System.out.printf("  - Throughput: %.2f req/sec (RPS)%n", throughputRps);
        System.out.println("----------------------------------------------------------------");
    }

    // =========================================================================
    // 4. STRESS: STREAM RANGE UNORDERED CONTAINERS
    // =========================================================================
    @Test
    void testStreamElementInRangeUnorderedStress() throws Exception {
        String vectorKey = "stress_range_vec_" + UUID.randomUUID();
        int initialSize = 5000;

        List<Payload> batch = new ArrayList<>(initialSize);
        for (int i = 0; i < initialSize; i++) {
            batch.add(Payload.of(("payload_range_data_" + i).getBytes()));
        }

        KeyHintData hint = client.createVector(bytes(vectorKey), batch).get();
        assertNotNull(hint);
        Thread.sleep(1000);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(THREAD_COUNT);

        LongAdder totalRangeRequests = new LongAdder();
        LongAdder totalItemsRead = new LongAdder();

        long startTime = System.nanoTime();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();

                    for (int j = 0; j < OPS_PER_THREAD; j++) {
                        int start = random.nextInt(0, 2000);
                        int end = start + random.nextInt(10, 500);

                        List<Payload> rangeResult = client.streamElementInRangeUnordered(
                                bytes(vectorKey), hint, ContainerType.VECTOR, start, end
                        ).get();

                        totalRangeRequests.increment();
                        if (rangeResult != null) {
                            totalItemsRead.add(rangeResult.size());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = finishLatch.await(10, TimeUnit.MINUTES);
        long durationNs = System.nanoTime() - startTime;
        executor.shutdown();

        assertTrue(completed, "Unordered Range Stream тест завершился по таймауту");

        double durationSec = durationNs / 1_000_000_000.0;
        double requestsRps = totalRangeRequests.sum() / durationSec;
        double itemsRps = totalItemsRead.sum() / durationSec;

        System.out.println("----------------------------------------------------------------");
        System.out.printf("[RANGE UNORDERED STRESS] Executed in: %.2f sec%n", durationSec);
        System.out.printf("  - Total Range Requests: %d (%.2f req/sec)%n", totalRangeRequests.sum(), requestsRps);
        System.out.printf("  - Total Elements Fetched: %d (%.2f items/sec)%n", totalItemsRead.sum(), itemsRps);
        System.out.println("----------------------------------------------------------------");
    }


}