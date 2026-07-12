package com.hurricache.client.cluster;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.grpc.KeyHint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

public class AtomicPerfStressTest {

    private static final int WORKER_THREADS = 16;
    private static final int DURATION_SECONDS = 15; // Время теста на один пресет ключей
    private static final int MAX_IN_FLIGHT_PER_THREAD = 120; // Окно отправки

    private static FastCacheAsyncSmartClient client;

    private static class AtomicMetrics {
        final LongAdder successOps = new LongAdder(); // cite: 6
        final LongAdder failedOps = new LongAdder();  // cite: 6
    }

    @BeforeAll
    public static void setup() {
        // Инициализируем Smart-клиент
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(15);
            }
        };
    }

    @AfterAll
    public static void teardown() {
        if (client != null) {
            client.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000, 10000,100000})
    void testAtomicAddStressUnderLoad(int keyCount) throws Exception {
        System.out.printf("%n=== STARTING STRESS TEST FOR %d ATOMICS ===%n", keyCount);

        // 1. Инициализация пула ключей и хинтов на сервере
        String[] keys = new String[keyCount];
        KeyHint[] hints = new KeyHint[keyCount];

        // Создаем атомики параллельно, чтобы не тратить время теста на подготовку
        ExecutorService initPool = Executors.newFixedThreadPool(32);
        CountDownLatch initLatch = new CountDownLatch(keyCount);

        for (int i = 0; i < keyCount; i++) {
            final int idx = i;
            keys[idx] = "stress_atomic_" + keyCount + "_" + idx + "_" + System.currentTimeMillis();
            initPool.submit(() -> {
                try {
                    // Первичное создание атомика со значением 0
                    byte[] keyBytes = keys[idx].getBytes(StandardCharsets.UTF_8);
                    hints[idx] = client.atomicCreate(keyBytes, 0L).get();
                    assertNotNull(hints[idx]);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    initLatch.countDown();
                }
            });
        }
        initLatch.await();
        initPool.shutdown();

        // 2. Подготовка инфраструктуры для нагрузки[cite: 6]
        AtomicMetrics metrics = new AtomicMetrics();
        ExecutorService workersPool = Executors.newFixedThreadPool(WORKER_THREADS); // cite: 6
        CountDownLatch startLatch = new CountDownLatch(1); // cite: 6
        AtomicBoolean running = new AtomicBoolean(true); // cite: 6

        // 3. Запуск потоков-нагружателей[cite: 6]
        for (int i = 0; i < WORKER_THREADS; i++) {
            workersPool.submit(() -> {
                try {
                    startLatch.await(); // Синхронный старт[cite: 6]

                    // Семафор на поток для удержания in-flight окна[cite: 6]
                    Semaphore inFlightWindow = new Semaphore(MAX_IN_FLIGHT_PER_THREAD); // cite: 6
                    long localIteration = 0;

                    while (running.get()) {
                        inFlightWindow.acquire(); // cite: 6

                        // Каждый поток распределяет свои вызовы по всему массиву ключей
                        int targetIdx = (int) (ThreadLocalRandom.current().nextLong(keyCount));
                        byte[] keyBytes = keys[targetIdx].getBytes(StandardCharsets.UTF_8);
                        KeyHint hint = hints[targetIdx];

                        // Асинхронный инкремент на +1
                        client.atomicAdd(keyBytes, hint, 1L)
                                .whenComplete((result, ex) -> {
                                    inFlightWindow.release(); // Освобождаем слот в окне[cite: 6]
                                    if (ex == null) {
                                        metrics.successOps.increment(); // cite: 6
                                    } else {
                                        metrics.failedOps.increment(); // cite: 6
                                    }
                                });
                        localIteration++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Даем отмашку на старт потоков[cite: 6]
        long startTime = System.nanoTime();
        startLatch.countDown(); // cite: 6

        // 4. Логгер промежуточной статистики раз в секунду[cite: 6]
        ScheduledExecutorService statLogger = Executors.newSingleThreadScheduledExecutor(); // cite: 6
        statLogger.scheduleAtFixedRate(() -> {
            long success = metrics.successOps.sum(); // cite: 6
            long failed = metrics.failedOps.sum(); // cite: 6
            double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0; // cite: 6

            System.out.printf("[STRESS-STAT] Keys: %5d | Time: %4.1fs | Success TPS: %8.2f | Failed: %d%n",
                              keyCount, elapsed, (success / elapsed), failed);
        }, 1, 1, TimeUnit.SECONDS); // cite: 6

        // Ждем окончания времени раунда[cite: 6]
        Thread.sleep(DURATION_SECONDS * 1000L); // cite: 6

        // Гасим нагрузку[cite: 6]
        running.set(false); // cite: 6
        workersPool.shutdown(); // cite: 6
        workersPool.awaitTermination(5, TimeUnit.SECONDS); // cite: 6
        statLogger.shutdownNow(); // cite: 6

        long totalTimeNs = System.nanoTime() - startTime;
        double totalTimeSec = totalTimeNs / 1_000_000_000.0; // cite: 6

        long totalSuccess = metrics.successOps.sum(); // cite: 6
        long totalFailed = metrics.failedOps.sum(); // cite: 6

        // 5. Финальный отчет по пресету ключей[cite: 6]
        System.out.printf("=================== ATOMIC PRESET %d FINAL STATS ===================%n", keyCount);
        System.out.printf("Duration:          %.2f seconds%n", totalTimeSec); // cite: 6
        System.out.printf("Keys Evaluated:    %d%n", keyCount);
        System.out.printf("Total Workers:     %d threads%n", WORKER_THREADS); // cite: 6
        System.out.printf("Successful Operations: %d%n", totalSuccess);
        System.out.printf("Failed Operations:     %d%n", totalFailed);
        System.out.printf("Avg Throughput:    %.2f ops/sec%n", (totalSuccess / totalTimeSec));
        System.out.printf("====================================================================%n");

        assertTrue(totalSuccess > 0, "Нагрузка не генерировалась, успешных операций 0");
        assertEquals(0, totalFailed, "Обнаружены ошибки gRPC при выполнении атомарных операций!");
    }
}