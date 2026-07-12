package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.KeyHint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class AtomicOperationsTest extends TestBaseCluster {

    private byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void atomicCreateAndStoreTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCreateStoreKey" + UUID.randomUUID();

        // 1. Создаем атомик со значением 42
        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(testKey,  42L).get();
        Assertions.assertNotNull(hint);
        Thread.sleep(150);

        // 2. Перезаписываем новое значение 100 через atomicStore
        KeyHint storeHint = client.setMode(Mode.MASTER)
                .atomicStore(testKey, hint, 100L).get();
        Assertions.assertNotNull(storeHint);

        // 3. Проверяем состояние на Master и Backup (добавив sleep для асинхронной репликации)
        Thread.sleep(150);

        // atomicOr с 0L возвращает значение ДО операции (т.е. актуальное состояние 100L)
        long currentMaster = client.setMode(Mode.MASTER)
                .atomicOr(testKey, hint, 0L).get();
        long currentBackup = client.setMode(Mode.BACKUP)
                .atomicOr(testKey, hint, 0L).get();

        Assertions.assertEquals(100L, currentMaster);
        Assertions.assertEquals(100L, currentBackup);
    }

    @Test
    void atomicExchangeTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicExchangeKey" + UUID.randomUUID();

        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(testKey,  10L).get();
        Thread.sleep(150);

        // Меняем 10 на 20, метод возвращает старое значение (10)
        long oldValue = client.setMode(Mode.MASTER)
                .atomicExchange(testKey, hint, 20L).get();
        Assertions.assertEquals(10L, oldValue);

        Thread.sleep(150);
        long currentBackup = client.setMode(Mode.BACKUP)
                .atomicOr(testKey, hint, 0L).get();
        Assertions.assertEquals(20L, currentBackup);
    }

    @Test
    void atomicAddAndSubTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddSubKey" + UUID.randomUUID();

        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(testKey, 50L).get();
        Thread.sleep(150);

        // Прибавляем 25 -> на сервере станет 75, но возвращается старое значение (50)
        long afterAdd = client.setMode(Mode.MASTER)
                .atomicAdd(testKey, hint, 25L).get();
        Assertions.assertEquals(50L, afterAdd);

        // Вычитаем 10 -> на сервере станет 65, но возвращается старое значение (75)
        long afterSub = client.setMode(Mode.MASTER)
                .atomicSub(testKey, hint, 10L).get();
        Assertions.assertEquals(75L, afterSub);

        Thread.sleep(150);
        long backupValue = client.setMode(Mode.BACKUP)
                .atomicOr(testKey, hint, 0L).get();
        Assertions.assertEquals(65L, backupValue);
    }

    @Test
    void atomicBitwiseOpsTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicBitwiseKey" + UUID.randomUUID();

        // Начальное значение: 12 (0b1100)
        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(testKey,  12L).get();
        Thread.sleep(150);

        // 1. AND с 10 (0b1010) -> на сервере станет 8 (0b1000), возвращает старое значение (12)
        long afterAnd = client.setMode(Mode.MASTER)
                .atomicAnd(testKey, hint, 10L).get();
        Assertions.assertEquals(12L, afterAnd);

        // 2. OR с 3 (0b0011) -> на сервере станет 11 (0b1011), возвращает старое значение (8)
        long afterOr = client.setMode(Mode.MASTER)
                .atomicOr(testKey, hint, 3L).get();
        Assertions.assertEquals(8L, afterOr); // ПОПРАВЛЕНО: возвращает 8, а не 11

        // 3. XOR с 15 (0b1111) -> на сервере станет 4 (0b0100), возвращает старое значение (11)
        long afterXor = client.setMode(Mode.MASTER)
                .atomicXor(testKey, hint, 15L).get();
        Assertions.assertEquals(11L, afterXor); // ПОПРАВЛЕНО: возвращает 11, а не 4

        Thread.sleep(150);
        long backupValue = client.setMode(Mode.BACKUP)
                .atomicOr(testKey, hint, 0L).get();
        Assertions.assertEquals(4L, backupValue);
    }

    @Test
    void atomicCompareAndSetSuccessTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasSuccessKey" + UUID.randomUUID();

        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(testKey,  500L).get();
        Thread.sleep(150);

        // Ожидаем 500, меняем на 600 -> должно пройти успешно
        AtomicCasRes res = client.setMode(Mode.MASTER)
                .atomicCompareAndSet(testKey, hint, 500L, 600L).get();

        Assertions.assertTrue(res.getResult());
        Assertions.assertFalse(res.hasExpected());

        Thread.sleep(150);
        long backupValue = client.setMode(Mode.BACKUP)
                .atomicOr(testKey, hint, 0L).get();
        Assertions.assertEquals(600L, backupValue);
    }

    @Test
    void atomicCompareAndSetFailureTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasFailKey" + UUID.randomUUID();

        KeyHint hint = client.setMode(Mode.MASTER)
                .atomicCreate(testKey,  500L).get();
        Thread.sleep(150);

        // Пытаемся поменять, ожидая ошибочные 999 вместо 500 -> успех должен быть false
        AtomicCasRes res = client.setMode(Mode.MASTER)
                .atomicCompareAndSet(testKey, hint, 999L, 600L).get();

        Assertions.assertFalse(res.getResult());
        // Должно вернуть актуальное текущее значение на сервере (500)
        Assertions.assertEquals(500L, res.getExpected().getVal());

        Thread.sleep(150);
        long backupValue = client.setMode(Mode.BACKUP)
                .atomicOr(testKey, hint, 0L).get();
        Assertions.assertEquals(500L, backupValue); // Значение не изменилось
    }

    @Test
    void atomicNonExistKeyTest() {
        String testKey = "atomicNonExistKey" + UUID.randomUUID();

        // Любая операция (кроме create) над несуществующим атомиком должна бросать NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .atomicAdd(testKey,  null,10L).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
}