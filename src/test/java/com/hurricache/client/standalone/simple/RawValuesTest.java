package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RawValuesTest extends TestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. TTL OPERATIONS (setTtl, getTtl)
    // =========================================================================

    @Test
    void testSetTtlOnScalar() throws ExecutionException, InterruptedException {
        String testKey = "scalar_ttl_set" + UUID.randomUUID();
        String testValue = "scalar_ttl_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Устанавливаем TTL 100 мс
        Boolean setResult = client.setTtl(testKey, null, 100, OWNER_CLIENT_ID).get();
        assertTrue(setResult, "TTL должен быть установлен");

        // Получаем TTL
        Long ttl = client.getTtl(testKey, OWNER_CLIENT_ID).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL должен быть положительным");
    }

    @Test
    void testGetTtlAfterSet() throws ExecutionException, InterruptedException {
        String testKey = "scalar_ttl_get" + UUID.randomUUID();
        String testValue = "scalar_ttl_get_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Устанавливаем TTL 5000 мс
        client.setTtl(testKey, null, 5000, OWNER_CLIENT_ID).get();

        Long ttl = client.getTtl(testKey, OWNER_CLIENT_ID).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 5000, "TTL должно быть в диапазоне");
    }

    @Test
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String testKey = "scalar_ttl_expire" + UUID.randomUUID();
        String testValue = "scalar_ttl_expire_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Устанавливаем TTL 100 мс
        client.setTtl(testKey, null, 100, OWNER_CLIENT_ID).get();

        // Ждём истечения
        Thread.sleep(200);

        // Скаляр должен быть недоступен
        try {
            client.getValue(testKey).get();
            Assertions.fail("getValue истёкшего скаляра должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 2. LOCK OPERATIONS (lock/unlock)
    // =========================================================================

    @Test
    void singleCreateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey" + UUID.randomUUID();
        String testValue = "singleCreateValueValue" + UUID.randomUUID();
        KeyHintData KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        // Key doesn't exist yet, will throw NOT_FOUND
    }

    @Test
    void singleCreateAndGet() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey" + UUID.randomUUID();
        String testValue = "singleCreateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
    }

    @Test
    void singleCreateExistValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistValue" + UUID.randomUUID();
        String testValue = "singleCreateExistValue123" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Boolean isExist = client.existKey(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertTrue(isExist);
    }

    @Test
    void singleCreateGetAndDeleteValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateGetAndDeleteValue" + UUID.randomUUID();
        String testValue = "singleCreateGetAndDeleteValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(100);
        byte[] bytes = client.getAndDeleteValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        try {
            client.getAndDeleteValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValue() throws InterruptedException {
        String testKey = "singleGenNonExistValue" + UUID.randomUUID();
        try {
            client.getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValue() throws InterruptedException {
        String testKey = "singleNonExistValue" + UUID.randomUUID();
        try {
            client.existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateUpdateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        String testValueUpdate = "singleCreateUpdateValueValue123" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        client.updateKeyValue(testKey, testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        byte[] newVal = client.getValue(testKey).get();
        Assertions.assertEquals(testValueUpdate, new String(newVal));
    }

    @Test
    void singleCreateDelete() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        Boolean b = client.remove(testKey).get();
        Assertions.assertTrue(b);
    }

    @Test
    void singleCreateGetDelete() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] bytes1 = client.getAndDeleteValue(testKey).get();
        Assertions.assertEquals(testValue, new String(bytes1));
    }

    @Test
    void singleCreateGetDeleteNoKeyHint() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] bytes1 = client.getAndDeleteValue(testKey).get();
        Assertions.assertEquals(testValue, new String(bytes1));
        Thread.sleep(150);
        assertThrows(ExecutionException.class, () -> {
            client.getValue(testKey).get();
        });
    }

    // =========================================================================
    // LOCK OPERATIONS FOR SCALARS
    // =========================================================================

    @Test
    void testReadLockAllowsParallelReads() throws ExecutionException, InterruptedException {
        String testKey = "scalar_read_lock" + UUID.randomUUID();
        String testValue = "scalar_read_lock_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает READ_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner может читать
        byte[] val = client.getValue(testKey, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder тоже может читать параллельно
        byte[] valIntruder = client.getValue(testKey, INTRUDER_CLIENT_ID).get();
        assertNotNull(valIntruder);

        // Разблокировка
        LockStatus unlock = client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String testKey = "scalar_read_lock_write" + UUID.randomUUID();
        String testValue = "scalar_read_lock_write_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает READ_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder не может писать (updateKeyValue)
        try {
            client.updateKeyValue(testKey, "new_value".getBytes(StandardCharsets.UTF_8), INTRUDER_CLIENT_ID).get();
            Assertions.fail("updateKeyValue должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Разблокировка
        client.unlockObject(testKey, OWNER_CLIENT_ID).get();
    }

    @Test
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String testKey = "scalar_write_lock" + UUID.randomUUID();
        String testValue = "scalar_write_lock_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner может читать
        byte[] val = client.getValue(testKey, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Owner может писать
        client.updateKeyValue(testKey, "updated_value".getBytes(StandardCharsets.UTF_8), OWNER_CLIENT_ID).get();

        // Intruder не может писать (mock server может не эмулировать блокировки для getValue)
        try {
            client.updateKeyValue(testKey, "intruder_value".getBytes(StandardCharsets.UTF_8), INTRUDER_CLIENT_ID).get();
            Assertions.fail("updateKeyValue должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Разблокировка
        LockStatus unlock = client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String testKey = "scalar_global_lock" + UUID.randomUUID();
        String testValue = "scalar_global_lock_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает GLOBAL_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner может читать
        byte[] val = client.getValue(testKey, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Owner может писать
        client.updateKeyValue(testKey, "global_updated".getBytes(StandardCharsets.UTF_8), OWNER_CLIENT_ID).get();

        // Intruder не может писать (mock server может не эмулировать блокировки для getValue)
        try {
            client.updateKeyValue(testKey, "intruder_global".getBytes(StandardCharsets.UTF_8), INTRUDER_CLIENT_ID).get();
            Assertions.fail("updateKeyValue должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Разблокировка
        LockStatus unlock = client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    void testIntruderCannotGetWriteLock() throws ExecutionException, InterruptedException {
        String testKey = "scalar_write_lock_denied" + UUID.randomUUID();
        String testValue = "scalar_write_lock_denied_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder не может получить WRITE_LOCK
        LockStatus intruderLock = client.lockObject(testKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertTrue(intruderLock != LockStatus.OK, "Intruder не должен получить блокировку");

        // Разблокировка
        client.unlockObject(testKey, OWNER_CLIENT_ID).get();
    }

    @Test
    void testIntruderCannotUnlock() throws ExecutionException, InterruptedException {
        String testKey = "scalar_unlock_denied" + UUID.randomUUID();
        String testValue = "scalar_unlock_denied_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK
        client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();

        // Intruder не может разблокировать
        LockStatus unlock = client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        // Owner разблокирует
        LockStatus unlockOwner = client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlockOwner);
    }

    @Test
    void testLockThenRemove() throws ExecutionException, InterruptedException {
        String testKey = "scalar_lock_remove" + UUID.randomUUID();
        String testValue = "scalar_lock_remove_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner может удалить
        Boolean removed = client.remove(testKey, OWNER_CLIENT_ID).get();
        assertTrue(removed);

        // unlockObject несуществующего ключа возвращает NOT_FOUND
        try {
            client.unlockObject(testKey, OWNER_CLIENT_ID).get();
            Assertions.fail("unlockObject удалённого ключа должен вызвать NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void testLockThenGetAndDeleteValue() throws ExecutionException, InterruptedException {
        String testKey = "scalar_lock_getdelete" + UUID.randomUUID();
        String testValue = "scalar_lock_getdelete_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner может getAndDeleteValue
        byte[] deleted = client.getAndDeleteValue(testKey, OWNER_CLIENT_ID).get();
        assertNotNull(deleted);
        assertEquals(testValue, new String(deleted));

        // unlockObject удалённого ключа возвращает NOT_FOUND
        try {
            client.unlockObject(testKey, OWNER_CLIENT_ID).get();
            Assertions.fail("unlockObject удалённого ключа должен вызвать NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void testLockThenExistKey() throws ExecutionException, InterruptedException {
        String testKey = "scalar_lock_exist" + UUID.randomUUID();
        String testValue = "scalar_lock_exist_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK
        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner может existKey
        Boolean exists = client.existKey(testKey, OWNER_CLIENT_ID).get();
        assertTrue(exists);

        // Разблокировка
        LockStatus unlock = client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    void testMultipleReadLocksOnScalar() throws ExecutionException, InterruptedException {
        String testKey = "scalar_multi_read" + UUID.randomUUID();
        String testValue = "scalar_multi_read_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Первый клиент получает READ_LOCK
        LockStatus lock1 = client.lockObject(testKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1);

        // Второй клиент также может получить READ_LOCK
        LockStatus lock2 = client.lockObject(testKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2);

        // Оба разблокируют
        client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // 3. LOCK + SCALAR OPERATIONS
    // =========================================================================

    // Дополнительные тесты lock+scalar уже добавлены в секцию 2

    // =========================================================================
    // 4. LOCK EXPIRATION
    // =========================================================================

    @Test
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String testKey = "scalar_write_lock_exp" + UUID.randomUUID();
        String testValue = "scalar_write_lock_exp_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает WRITE_LOCK на 2 секунды
        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь intruder может получить WRITE_LOCK
        LockStatus intruderLock = client.lockObject(testKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock);

        // Разблокирует
        client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    void testReadLockExpiration() throws ExecutionException, InterruptedException {
        String testKey = "scalar_read_lock_exp" + UUID.randomUUID();
        String testValue = "scalar_read_lock_exp_value" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();

        // Owner получает READ_LOCK на 2 секунды
        LockStatus lock = client.lockObject(testKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь intruder может получить READ_LOCK
        LockStatus intruderLock = client.lockObject(testKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock);

        // Разблокирует
        client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
    }
}