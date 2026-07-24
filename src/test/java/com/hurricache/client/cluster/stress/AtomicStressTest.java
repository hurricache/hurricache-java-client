package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.KeyHint;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AtomicStressTest extends TestBaseCluster {

    private byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void highContentionAtomicAddStressTest() throws Exception {
        String testKey = "stressAtomicKey_" + UUID.randomUUID();

        // 1. Инициализируем атомик со значением 0
        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(bytes(testKey), null, 0L).get();
        Assertions.assertNotNull(hint);

        int numberOfThreads = 16;       // Количество параллельных воркеров
        int incrementsPerThread = 500;  // Сколько инкрементов сделает каждый поток (итого 8000 операций)

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numberOfThreads);
        AtomicInteger failedRequests = new AtomicInteger(0);
        Thread.sleep(1000); //Ждем завершения репликации
        // 2. Накатываем нагрузку
        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Синхронный старт всех потоков для максимального contention
                    for (int j = 0; j < incrementsPerThread; j++) {
                        // Используем дефолтный или SMART режим, чтобы протестировать логику маршрутизации клиента
                        client.setMode(Mode.MASTER)
                                .atomicAdd(bytes(testKey), hint, 1L)
                                .get();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    failedRequests.incrementAndGet();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Одновременный залп!
        startLatch.countDown();

        // Ждем завершения теста (не более 30 секунд)
        boolean finishedCleanly = finishLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // 3. Проверки
        Assertions.assertTrue(finishedCleanly, "Тест завис или не уложился в таймаут!");
        Assertions.assertEquals(0, failedRequests.get(), "Были зафиксированы упавшие gRPC запросы!");

        // Даем время асинхронной репликации долететь до бэкапа
        Thread.sleep(1000);

        long expectedFinalValue = (long) numberOfThreads * incrementsPerThread;

        // Читаем финальное состояние с Master и с Backup через атомарный bitwise OR
        long finalMasterValue = client.setMode(Mode.MASTER)
                .atomicOr(bytes(testKey), hint, 0L).get();
        long finalBackupValue = client.setMode(Mode.BACKUP)
                .atomicOr(bytes(testKey), hint, 0L).get();

        System.out.printf("[STRESS REPORT] Expected: %d | Master: %d | Backup: %d%n",
                          expectedFinalValue, finalMasterValue, finalBackupValue);

        Assertions.assertEquals(expectedFinalValue, finalMasterValue, "Данные на Master разошлись из-за Race Condition!");
        Assertions.assertEquals(expectedFinalValue, finalBackupValue, "Репликация на Backup потеряла часть инкрементов!");
    }
}