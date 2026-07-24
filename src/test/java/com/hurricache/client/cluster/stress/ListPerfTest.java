package com.hurricache.client.cluster.stress;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.Mode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListPerfTest {

    private static final String LIST_NAME = "perf-test-list-" + System.currentTimeMillis();
    private static final int MESSAGE_SIZE = 100;

    // Конфигурация нагрузки
    private static final int WRITER_THREADS = 8;
    private static final int READER_THREADS = 8;
    private static final int DURATION_SECONDS = 60;

    // Ограничитель очереди in-flight асинхронных запросов на один поток
    // Позволяет утилизировать сеть, не забивая RAM бесконечными тасками
    private static final int MAX_IN_FLIGHT_PER_THREAD = 100;

    private static FastCacheAsyncSmartClient client;
    private static com.hurricache.grpc.KeyHint queueKeyHint;

    // Раздельные метрики производительности
    private static class PerfMetrics {
        final LongAdder produced = new LongAdder();
        final LongAdder failedWrites = new LongAdder();
        final LongAdder consumed = new LongAdder();
        final LongAdder failedReads = new LongAdder();
        final LongAdder emptyReads = new LongAdder();
    }

    @BeforeAll
    public static void setup() throws Exception {
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, Duration.ofSeconds(5)) {
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(15);
            }
        };
        queueKeyHint = client.createList(LIST_NAME).get();
        assertNotNull(queueKeyHint, "Queue must be created");
    }

    @AfterAll
    public static void teardown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    void testConcurrentListProducerConsumer() throws Exception {
        PerfMetrics metrics = new PerfMetrics();
        ExecutorService pool = Executors.newFixedThreadPool(WRITER_THREADS + READER_THREADS);

        // Сигнал для одновременного старта и стопа
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        // 1. Запуск Писателей (Producers)
        for (int i = 0; i < WRITER_THREADS; i++) {
            int writerId = i;
            AtomicBoolean finalRunning = running;
            pool.submit(() -> {
                try {
                    startLatch.await();
                    writeMessagesLoop(writerId, metrics, () -> finalRunning.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // 2. Запуск Читателей (Consumers)
        for (int i = 0; i < READER_THREADS; i++) {
            int readerId = i;
            AtomicBoolean finalRunning1 = running;
            pool.submit(() -> {
                try {
                    startLatch.await();
                    readMessagesLoop(readerId, metrics, () -> finalRunning1.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Даем отмашку на старт
        long startTime = System.nanoTime();
        startLatch.countDown();

        // 3. Вывод промежуточной статистики каждую секунду
        ScheduledExecutorService statLogger = Executors.newSingleThreadScheduledExecutor();
        statLogger.scheduleAtFixedRate(() -> {
            long p = metrics.produced.sum();
            long c = metrics.consumed.sum();
            long er = metrics.emptyReads.sum();
            double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;

            System.out.printf("[STAT] Time: %.1fs | Write TPS: %.2f | Read TPS: %.2f (Empty: %d) | Queue Delta: %d%n",
                              elapsed, (p / elapsed), (c / elapsed), er, (p - c));
        }, 1, 1, TimeUnit.SECONDS);

        // Ждем окончания теста
        Thread.sleep(DURATION_SECONDS * 1000L);

        // Останавливаем циклы
        running.getAndSet(false);

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        statLogger.shutdownNow();

        long totalTimeNs = System.nanoTime() - startTime;
        double totalTimeSec = totalTimeNs / 1_000_000_000.0;

        // 4. Финальный детализированный отчет
        System.out.printf("\n=================== FINAL STATS ===================\n");
        System.out.printf("Duration:         %.2f seconds\n", totalTimeSec);
        System.out.printf("Writers (%d th):   Produced: %d | Failed: %d | Speed: %.2f msg/sec\n",
                          WRITER_THREADS, metrics.produced.sum(), metrics.failedWrites.sum(), (metrics.produced.sum() / totalTimeSec));
        System.out.printf("Readers (%d th):   Consumed: %d | Failed: %d | Empty: %d | Speed: %.2f msg/sec\n",
                          READER_THREADS, metrics.consumed.sum(), metrics.failedReads.sum(), metrics.emptyReads.sum(), (metrics.consumed.sum() / totalTimeSec));
        System.out.printf("Unconsumed Msg:   %d\n", (metrics.produced.sum() - metrics.consumed.sum()));
        System.out.printf("===================================================\n");

        assertTrue(metrics.produced.sum() > 0, "Producers should write something");
        assertTrue(metrics.consumed.sum() > 0, "Consumers should read something");
    }

    private void writeMessagesLoop(int writerId, PerfMetrics metrics, java.util.function.BooleanSupplier isRunning) {
        // Ограничитель параллельных запросов (Semaphore) удерживает поток от бесконечной генерации
        Semaphore inFlightWindow = new Semaphore(MAX_IN_FLIGHT_PER_THREAD);
        int i = 0;

        while (isRunning.getAsBoolean()) {
            try {
                inFlightWindow.acquire(); // Ждем свободного слота в окне отправки

                byte[] payload = generate100ByteString(writerId + "-" + i++);
                client.setMode(Mode.LB_SMART).addElementToTail(LIST_NAME, queueKeyHint, List.of(payload)).whenComplete((success, ex) -> {
                    inFlightWindow.release(); // Освобождаем слот сразу по завершению сетевой операции
                    if (ex == null && success) {
                        metrics.produced.increment();
                    } else {
                        metrics.failedWrites.increment();
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void readMessagesLoop(int readerId, PerfMetrics metrics, java.util.function.BooleanSupplier isRunning) {
        Semaphore inFlightWindow = new Semaphore(MAX_IN_FLIGHT_PER_THREAD);

        while (isRunning.getAsBoolean()) {
            try {
                inFlightWindow.acquire();

                client.getAndRemoveFront(LIST_NAME, queueKeyHint).whenComplete((resp, ex) -> {
                    inFlightWindow.release();
                    if (ex != null) {
                        metrics.failedReads.increment();
                    } else if (resp != null && resp.length > 0) {
                        metrics.consumed.increment();
                    } else {
                        metrics.emptyReads.increment(); // Очередь пуста, писатель не успевает
                        // Делаем микропаузу ПРИ ПУСТОЙ очереди, чтобы не спамить CPU холостыми сетевыми вызовами
                        LockSupport.parkNanos(100_000); // 0.1 ms
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private byte[] generate100ByteString(String prefix) {
        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] result = new byte[MESSAGE_SIZE];
        System.arraycopy(prefixBytes, 0, result, 0, Math.min(prefixBytes.length, MESSAGE_SIZE));
        for (int i = prefixBytes.length; i < MESSAGE_SIZE; i++) {
            result[i] = 'x';
        }
        return result;
    }
}