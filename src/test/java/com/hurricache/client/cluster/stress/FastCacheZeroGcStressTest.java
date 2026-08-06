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
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class FastCacheZeroGcStressTest {

    private final String prefix = UUID.randomUUID() + "-" + System.currentTimeMillis() + ":::";

    private final int THREAD_COUNT = 64;
    private final int PRECONDITION_KEYS = 320_000;
    private final int TEST_DURATION_MINUTES = 5;
    private final int REPORTING_INTERVAL_SECONDS = 10;
    private final int MAX_IN_FLIGHT_PER_THREAD = 1_000;

    private static final byte[] PREALLOCATED_VALUE = "value_payload_placeholder_for_high_load_testing".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREALLOCATED_UPDATE = "value_payload_placeholder_for_high_load_testing_updated".getBytes(StandardCharsets.UTF_8);

    private FastCacheAsyncSmartClient client;
    private ExecutorService executor;
    private ScheduledExecutorService reporterExecutor;

    // Пул индексированных ключей
    private KeyEntry[] keyArray;
    private final AtomicLong keySequenceCounter = new AtomicLong(0);

    // Массив счетчиков [Succ, Err] для каждой операции: 0:CREATE, 1:UPDATE, 2:GET, 3:DELETE
    private final AtomicInteger[] succCounters = new AtomicInteger[4];
    private final AtomicInteger[] errCounters = new AtomicInteger[4];

    @BeforeEach
    void setUp() throws InterruptedException {
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
        reporterExecutor = Executors.newSingleThreadScheduledExecutor();

        for (int i = 0; i < 4; i++) {
            succCounters[i] = new AtomicInteger(0);
            errCounters[i] = new AtomicInteger(0);
        }

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
    void optimizedZeroGcStressTest() throws InterruptedException {
        keyArray = new KeyEntry[PRECONDITION_KEYS];

        // ==========================================
        // PRECONDITION: Прогрев пула
        // ==========================================
        System.out.println("=== PRECONDITION: Populating Key Pool ===");
        long preconditionStart = System.currentTimeMillis();
        CountDownLatch preconditionLatch = new CountDownLatch(THREAD_COUNT);

        int keysPerThread = PRECONDITION_KEYS / THREAD_COUNT;
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    int startIdx = threadId * keysPerThread;
                    for (int j = 0; j < keysPerThread; j++) {
                        int currentIdx = startIdx + j;
                        String key = prefix + "init:" + currentIdx;
                        try {
                            KeyHintData hint = client.createKeyValue(key, PREALLOCATED_VALUE).get(5, TimeUnit.SECONDS);
                            if (hint != null) {
                                keyArray[currentIdx] = new KeyEntry(key, hint);
                            }
                        } catch (Exception ignored) {}
                    }
                } finally {
                    preconditionLatch.countDown();
                }
            });
        }

        preconditionLatch.await(5, TimeUnit.MINUTES);
        System.out.println(String.format("Precondition Done in %d ms.\n", System.currentTimeMillis() - preconditionStart));

        // ==========================================
        // HIGH-LOAD SPRINT (с ротацией ключей)
        // ==========================================
        System.out.println(String.format("=== STARTING HIGH-LOAD SPRINT (%d MIN) ===", TEST_DURATION_MINUTES));

        CountDownLatch mainLatch = new CountDownLatch(THREAD_COUNT);
        long startTime = System.currentTimeMillis();
        long endTimeTarget = startTime + TimeUnit.MINUTES.toMillis(TEST_DURATION_MINUTES);

        reporterExecutor.scheduleAtFixedRate(
                new MetricsReporter(succCounters, errCounters),
                REPORTING_INTERVAL_SECONDS, REPORTING_INTERVAL_SECONDS, TimeUnit.SECONDS
        );

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                Semaphore inFlightThrottle = new Semaphore(MAX_IN_FLIGHT_PER_THREAD);

                try {
                    while (System.currentTimeMillis() < endTimeTarget) {
                        inFlightThrottle.acquire();

                        int roll = random.nextInt(100);
                        int opIndex = (roll < 20) ? 0 : (roll < 30) ? 1 : (roll < 95) ? 2 : 3;

                        int randomSlot = random.nextInt(PRECONDITION_KEYS);
                        KeyEntry entry = keyArray[randomSlot];

                        // Защита от NOP если элемент пула еще не инициализировался
                        if (entry == null && opIndex != 0) {
                            inFlightThrottle.release();
                            continue;
                        }

                        String key;
                        KeyHintData hint;
                        if (opIndex == 0) { // CREATE
                            key = prefix + "dyn:" + threadId + ":" + keySequenceCounter.incrementAndGet();
                            hint = null;
                        } else {
                            key = entry.key;
                            hint = entry.hint;
                        }

                        CompletableFuture<?> future;
                        switch (opIndex) {
                            case 0:
                                future = client.createKeyValue(key, PREALLOCATED_VALUE);
                                break;
                            case 1:
                                future = client.updateKeyValue(key, hint, PREALLOCATED_UPDATE);
                                break;
                            case 2:
                                future = client.getValue(key, hint);
                                break;
                            case 3:
                            default:
                                future = client.remove(key, hint);
                                break;
                        }

                        // Коллбэк обработки ответа и ротации живого пула
                        final String finalKey = key;
                        future.whenComplete((res, ex) -> {
                            inFlightThrottle.release();

                            boolean isSuccess = (ex == null && res != null && !(res instanceof Boolean && !((Boolean) res)));

                            if (isSuccess) {
                                succCounters[opIndex].incrementAndGet();

                                // Динамическая подмена ключа в пуле при успешном CREATE
                                if (opIndex == 0 && res instanceof KeyHintData) {
                                    keyArray[randomSlot] = new KeyEntry(finalKey, (KeyHintData) res);
                                }
                            } else {
                                errCounters[opIndex].incrementAndGet();
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
        private final AtomicInteger[] succ;
        private final AtomicInteger[] err;
        private long lastCheckTime = System.currentTimeMillis();
        private int lastTotalOps = 0;

        public MetricsReporter(AtomicInteger[] succ, AtomicInteger[] err) {
            this.succ = succ;
            this.err = err;
        }

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            long intervalMs = now - lastCheckTime;
            lastCheckTime = now;

            int currentTotal = 0;
            int totalFailures = 0;
            for (int i = 0; i < 4; i++) {
                currentTotal += succ[i].get() + err[i].get();
                totalFailures += err[i].get();
            }

            int intervalOps = currentTotal - lastTotalOps;
            lastTotalOps = currentTotal;

            double intervalRps = (double) intervalOps / (intervalMs / 1000.0);
            double failurePercent = currentTotal > 0 ? ((double) totalFailures / currentTotal) * 100 : 0;

            System.out.println(String.format(
                    "[%tT] Interval RPS: %8.2f | Last 10s Ops: %8d | Total Ops: %10d | Failures: %d (%.2f%%)",
                    now, intervalRps, intervalOps, currentTotal, totalFailures, failurePercent));
            System.out.println(String.format(
                    "       └─ Succ/Err: CREATE[%d/%d] UPDATE[%d/%d] GET[%d/%d] DELETE[%d/%d]",
                    succ[0].get(), err[0].get(), succ[1].get(), err[1].get(),
                    succ[2].get(), err[2].get(), succ[3].get(), err[3].get()));
        }
    }
}