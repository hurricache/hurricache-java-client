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
import java.util.concurrent.atomic.AtomicLong;

public class FastCacheHighLoadStressTest {

    private final String prefix = UUID.randomUUID() + "-" + System.currentTimeMillis() + ":::";

    // Агрессивные настройки нагрузки
    private final int THREAD_COUNT = 64;                       // Увеличено число рабочих потоков
    private final int PRECONDITION_KEYS_PER_THREAD = 5_000;    // Начальный пул
    private final int TEST_DURATION_MINUTES = 5;
    private final int REPORTING_INTERVAL_SECONDS = 10;
    private final int MAX_IN_FLIGHT_PER_THREAD = 1_000;        // Окно асинхронных запросов в полете
    private final double MAX_ALLOWED_FAILURE_RATE = 0.30;

    private static final byte[] PREALLOCATED_VALUE = "value_payload_placeholder_for_high_load_testing".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREALLOCATED_UPDATE = "value_payload_placeholder_for_high_load_testing_updated".getBytes(StandardCharsets.UTF_8);

    private FastCacheAsyncSmartClient client;
    private ExecutorService executor;
    private ScheduledExecutorService reporterExecutor;

    @BeforeEach
    void setUp() throws InterruptedException {
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
        reporterExecutor = Executors.newSingleThreadScheduledExecutor();

        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(10);
            }
        };
        client.setMode(Mode.MASTER_THAN_BACKUP);

        while (!client.getReadyFlag()) {
            Thread.sleep(100);
        }
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.shutdown();
        if (executor != null) executor.shutdownNow();
        if (reporterExecutor != null) reporterExecutor.shutdownNow();
    }

    @Test
    void highPerformanceMixedLoadTest() throws InterruptedException {
        // O(1) структура данных для пула ключей и хинтов без блокировок
        List<KeyEntry> keyPool = new CopyOnWriteArrayList<>();
        AtomicLong keySequenceCounter = new AtomicLong(0);

        // ==========================================
        // PRECONDITION: Быстрое наполнение пула ключей
        // ==========================================
        System.out.println("=== PRECONDITION: Fast Populating Initial Key Pool ===");
        long preconditionStart = System.currentTimeMillis();
        CountDownLatch preconditionLatch = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < PRECONDITION_KEYS_PER_THREAD; j++) {
                        String key = getTestKey(threadId, keySequenceCounter.incrementAndGet());
                        try {
                            KeyHintData hint = client.createKeyValue(key, PREALLOCATED_VALUE).get(5, TimeUnit.SECONDS);
                            if (hint != null) {
                                keyPool.add(new KeyEntry(key, hint));
                            }
                        } catch (Exception ignored) {}
                    }
                } finally {
                    preconditionLatch.countDown();
                }
            });
        }

        preconditionLatch.await(5, TimeUnit.MINUTES);
        System.out.println(String.format("Precondition Done in %d ms. Initial Key Pool Size: %d\n",
                                         System.currentTimeMillis() - preconditionStart, keyPool.size()));

        Assertions.assertFalse(keyPool.isEmpty(), "Precondition failed: Pool is empty!");

        // Быстрый массив для выборки за O(1)
        final KeyEntry[] keyArray = keyPool.toArray(new KeyEntry[0]);
        final int initialPoolSize = keyArray.length;

        // ==========================================
        // HIGH-LOAD SPRINT (15 MIN)
        // ==========================================
        System.out.println(String.format("=== STARTING HIGH-LOAD SPRINT (%d MIN) ===", TEST_DURATION_MINUTES));
        System.out.println("Target Profile: 20% CREATE, 10% UPDATE, 65% GET, 5% DELETE");

        CountDownLatch mainLatch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger createSuccess = new AtomicInteger(0), createErrors = new AtomicInteger(0);
        AtomicInteger updateSuccess = new AtomicInteger(0), updateErrors = new AtomicInteger(0);
        AtomicInteger getSuccess    = new AtomicInteger(0), getErrors    = new AtomicInteger(0);
        AtomicInteger deleteSuccess = new AtomicInteger(0), deleteErrors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long endTimeTarget = startTime + TimeUnit.MINUTES.toMillis(TEST_DURATION_MINUTES);

        // Репортер раз в 10 секунд
        MetricsReporter reporter = new MetricsReporter(
                createSuccess, createErrors, updateSuccess, updateErrors,
                getSuccess, getErrors, deleteSuccess, deleteErrors
        );
        reporterExecutor.scheduleAtFixedRate(reporter, REPORTING_INTERVAL_SECONDS, REPORTING_INTERVAL_SECONDS, TimeUnit.SECONDS);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                Semaphore inFlightThrottle = new Semaphore(MAX_IN_FLIGHT_PER_THREAD);

                try {
                    while (System.currentTimeMillis() < endTimeTarget) {
                        inFlightThrottle.acquire(); // Контроль окна отправки

                        int roll = random.nextInt(100);
                        OpType opType = (roll < 20) ? OpType.CREATE :
                                        (roll < 30) ? OpType.UPDATE :
                                        (roll < 95) ? OpType.GET : OpType.DELETE;

                        // Выбор ключа за O(1)
                        KeyEntry entry = keyArray[random.nextInt(initialPoolSize)];
                        String key = (opType == OpType.CREATE) ? getTestKey(threadId, keySequenceCounter.incrementAndGet()) : entry.key;
                        KeyHintData hint = entry.hint;

                        CompletableFuture<?> future;
                        switch (opType) {
                            case CREATE:
                                future = client.createKeyValue(key, PREALLOCATED_VALUE);
                                break;
                            case UPDATE:
                                future = client.setMode(Mode.LB_SMART).updateKeyValue(key, hint, PREALLOCATED_UPDATE);
                                break;
                            case GET:
                                future = client.setMode(Mode.LB_SMART).getValue(key, hint);
                                break;
                            case DELETE:
                            default:
                                future = client.setMode(Mode.LB_SMART).remove(key, hint);
                                break;
                        }

                        // Неблокирующая обработка результата (In-flight saturation)
                        final OpType currentOp = opType;
                        future.whenComplete((res, ex) -> {
                            inFlightThrottle.release();

                            if (ex != null || res == null || (res instanceof Boolean && !((Boolean) res))) {
                                switch (currentOp) {
                                    case CREATE: createErrors.incrementAndGet(); break;
                                    case UPDATE: updateErrors.incrementAndGet(); break;
                                    case GET:    getErrors.incrementAndGet(); break;
                                    case DELETE: deleteErrors.incrementAndGet(); break;
                                }
                            } else {
                                switch (currentOp) {
                                    case CREATE: createSuccess.incrementAndGet(); break;
                                    case UPDATE: updateSuccess.incrementAndGet(); break;
                                    case GET:    getSuccess.incrementAndGet(); break;
                                    case DELETE: deleteSuccess.incrementAndGet(); break;
                                }
                            }
                        });
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    mainLatch.countDown();
                }
            });
        }

        mainLatch.await(TEST_DURATION_MINUTES + 2, TimeUnit.MINUTES);
        reporterExecutor.shutdown();

        long actualEndTime = System.currentTimeMillis();
        int totalSuccess = createSuccess.get() + updateSuccess.get() + getSuccess.get() + deleteSuccess.get();
        int totalErrors  = createErrors.get() + updateErrors.get() + getErrors.get() + deleteErrors.get();

        printFinalResults(startTime, actualEndTime, totalSuccess, totalErrors,
                          createSuccess.get(), createErrors.get(), updateSuccess.get(), updateErrors.get(),
                          getSuccess.get(), getErrors.get(), deleteSuccess.get(), deleteErrors.get());

        double failureRate = (double) totalErrors / (totalSuccess + totalErrors);
        Assertions.assertTrue(failureRate <= MAX_ALLOWED_FAILURE_RATE,
                              String.format("Failure rate limit exceeded! Expected <= %.2f%%, got %.2f%%",
                                            MAX_ALLOWED_FAILURE_RATE * 100, failureRate * 100));
    }

    private String getTestKey(int threadId, long opId) {
        return prefix + "mix:" + threadId + ":" + opId;
    }

    private static class KeyEntry {
        final String key;
        final KeyHintData hint;
        KeyEntry(String key, KeyHintData hint) {
            this.key = key;
            this.hint = hint;
        }
    }

    private static class MetricsReporter implements Runnable {
        private final AtomicInteger cSucc, cErr, uSucc, uErr, gSucc, gErr, dSucc, dErr;
        private long lastCheckTime = System.currentTimeMillis();
        private int lastTotalOps = 0;

        public MetricsReporter(AtomicInteger cSucc, AtomicInteger cErr, AtomicInteger uSucc, AtomicInteger uErr,
                               AtomicInteger gSucc, AtomicInteger gErr, AtomicInteger dSucc, AtomicInteger dErr) {
            this.cSucc = cSucc; this.cErr = cErr;
            this.uSucc = uSucc; this.uErr = uErr;
            this.gSucc = gSucc; this.gErr = gErr;
            this.dSucc = dSucc; this.dErr = dErr;
        }

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            long intervalMs = now - lastCheckTime;
            lastCheckTime = now;

            int currentTotal = cSucc.get() + cErr.get() + uSucc.get() + uErr.get() + gSucc.get() + gErr.get() + dSucc.get() + dErr.get();
            int intervalOps = currentTotal - lastTotalOps;
            lastTotalOps = currentTotal;

            double intervalRps = (double) intervalOps / (intervalMs / 1000.0);
            int totalFailures = cErr.get() + uErr.get() + gErr.get() + dErr.get();
            double failurePercent = currentTotal > 0 ? ((double) totalFailures / currentTotal) * 100 : 0;

            System.out.println(String.format(
                    "[%tT] Interval RPS: %8.2f | Last 10s Ops: %8d | Total Ops: %10d | Total Failures: %d (%.2f%%)",
                    now, intervalRps, intervalOps, currentTotal, totalFailures, failurePercent));
            System.out.println(String.format(
                    "       └─ Succ/Err: CREATE[%d/%d] UPDATE[%d/%d] GET[%d/%d] DELETE[%d/%d]",
                    cSucc.get(), cErr.get(), uSucc.get(), uErr.get(), gSucc.get(), gErr.get(), dSucc.get(), dErr.get()));
        }
    }

    private void printFinalResults(long startTime, long endTime, int totalSuccess, int totalErrors,
                                   int cSucc, int cErr, int uSucc, int uErr,
                                   int gSucc, int gErr, int dSucc, int dErr) {
        long durationMs = endTime - startTime;
        int totalOps = totalSuccess + totalErrors;
        double opsPerSec = (double) totalOps / (durationMs / 1000.0);
        double failureRate = ((double) totalErrors / totalOps) * 100;

        System.out.println("\n==========================================================================");
        System.out.println("--- FINAL HIGH-LOAD STRESS TEST RESULTS ---");
        System.out.println("==========================================================================");
        System.out.println("Total Duration:            " + (durationMs / 1000) + " seconds");
        System.out.println("Total Operations Executed: " + totalOps);
        System.out.println(String.format("Average Throughput (RPS):  %.2f ops/sec", opsPerSec));
        System.out.println("--------------------------------------------------------------------------");
        System.out.println(String.format("Total Successful:          %d (%.2f%%)", totalSuccess, (100.0 - failureRate)));
        System.out.println(String.format("Total Failed:              %d (%.2f%%)", totalErrors, failureRate));
        System.out.println("--------------------------------------------------------------------------");
        System.out.println("FINAL BREAKDOWN BY OPERATION:");
        printOpMetrics("CREATE", cSucc, cErr);
        printOpMetrics("UPDATE", uSucc, uErr);
        printOpMetrics("GET",    gSucc, gErr);
        printOpMetrics("DELETE", dSucc, dErr);
        System.out.println("==========================================================================\n");
    }

    private void printOpMetrics(String opName, int success, int errors) {
        int total = success + errors;
        double successRate = total > 0 ? ((double) success / total) * 100 : 0;
        double errorRate = total > 0 ? ((double) errors / total) * 100 : 0;
        System.out.println(String.format(" - %-7s | Total: %-9d | Success: %-9d (%.2f%%) | Failed: %-8d (%.2f%%)",
                                         opName, total, success, successRate, errors, errorRate));
    }

    private enum OpType { CREATE, UPDATE, GET, DELETE }
}