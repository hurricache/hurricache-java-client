package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class AtomicOperationsTest extends TestBaseCluster {

    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final long REPLICATION_DELAY_MS = 100;


    @Test
    void atomicCreateAndStoreTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCreateStoreKey" + UUID.randomUUID();

        // 1. Создаем атомик со значением 42
        KeyHintData hint = client.atomicCreate(testKey,  42L).get();
        Assertions.assertNotNull(hint);
        Thread.sleep(150);

        // 2. Перезаписываем новое значение 100 через atomicStore
        KeyHintData storeHint = client
                .atomicStore(testKey, hint, 100L).get();
        Assertions.assertNotNull(storeHint);

        // 3. Проверяем состояние на Master и Backup через atomicLoad
        Thread.sleep(150);

        long currentMaster = client
                .atomicLoad(testKey).get();
        long currentBackup = client
                .atomicLoad(testKey).get();

        Assertions.assertEquals(100L, currentMaster);
        Assertions.assertEquals(100L, currentBackup);
    }

    @Test
    void atomicExchangeTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicExchangeKey" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(testKey,  10L).get();
        Thread.sleep(150);

        // Меняем 10 на 20, метод возвращает старое значение (10)
        long oldValue = client
                .atomicExchange(testKey, hint, 20L).get();
        Assertions.assertEquals(10L, oldValue);

        Thread.sleep(150);
        // Verify via atomicLoad
        long currentBackup = client
                .atomicLoad(testKey).get();
        Assertions.assertEquals(20L, currentBackup);
    }

    @Test
    void atomicAddAndSubTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddSubKey" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 50L).get();
        Thread.sleep(150);

        // Прибавляем 25 -> на сервере станет 75
        long afterAdd = client
                .atomicAdd(bytes(testKey), hint, 25L).get();

        // Вычитаем 10 -> на сервере станет 65
        long afterSub = client
                .atomicSub(bytes(testKey), hint, 10L).get();

        Thread.sleep(150);
        // Verify via atomicLoad
        long backupValue = client
                .atomicLoad(testKey).get();
        Assertions.assertEquals(65L, backupValue);
    }

    @Test
    void atomicAddAndSubTest1() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddSubKey" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 50L).get();
        Thread.sleep(150);

        // Прибавляем 25 -> на сервере станет 75
        long afterAdd = client
                .atomicAdd(bytes(testKey), hint, 25L).get();
        Thread.sleep(150);
        // Вычитаем 10 -> на сервере станет 65
        long afterSub = client
                .atomicSub(bytes(testKey), hint, 10L).get();

        Thread.sleep(150);
        // Verify via atomicLoad
        long backupValue = client
                .atomicLoad(testKey).get();
        Assertions.assertEquals(65L, backupValue);
    }


    @Test
    void atomicBitwiseOpsTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicBitwiseKey" + UUID.randomUUID();

        // Начальное значение: 12 (0b1100)
        KeyHintData hint = client.atomicCreate(testKey,  12L).get();
        Thread.sleep(150);

        // 1. AND с 10 (0b1010) -> на сервере станет 8 (0b1000)
        client
                .atomicAnd(bytes(testKey), hint, 10L).get();

        // 2. OR с 3 (0b0011) -> на сервере станет 11 (0b1011)
        client
                .atomicOr(bytes(testKey), hint, 3L).get();

        // 3. XOR с 15 (0b1111) -> на сервере станет 4 (0b0100)
        client
                .atomicXor(bytes(testKey), hint, 15L).get();

        Thread.sleep(150);
        // Verify via atomicLoad: 8 OR 3 = 11, 11 XOR 15 = 4
        long backupValue = client
                .atomicLoad(testKey).get();
        Assertions.assertEquals(4L, backupValue);
    }

    @Test
    void atomicCompareAndSetSuccessTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasSuccessKey" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(testKey,  500L).get();
        Thread.sleep(150);

        // Ожидаем 500, меняем на 600 -> должно пройти успешно
        AtomicCasRes res = client
                .atomicCompareAndSet(testKey, hint, 500L, 600L).get();

        Assertions.assertTrue(res.getResult());
        Assertions.assertFalse(res.hasExpected());

        Thread.sleep(150);
        long backupValue = client
                .atomicOr(bytes(testKey), hint, 0L).get();
        Assertions.assertEquals(600L, backupValue);
    }

    @Test
    void atomicCompareAndSetFailureTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasFailKey" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(testKey,  500L).get();
        Thread.sleep(150);

        // Пытаемся поменять, ожидая ошибочные 999 вместо 500 -> успех должен быть false
        AtomicCasRes res = client
                .atomicCompareAndSet(testKey, hint, 999L, 600L).get();

        Assertions.assertFalse(res.getResult());
        // Должно вернуть актуальное текущее значение на сервере (500)
        Assertions.assertEquals(500L, res.getExpected().getVal());

        Thread.sleep(500);
        long backupValue = client
                .atomicOr(bytes(testKey), hint, 0L).get();
        Assertions.assertEquals(500L, backupValue); // Значение не изменилось
    }

    @Test
    void atomicNonExistKeyTest() {
        String testKey = "atomicNonExistKey" + UUID.randomUUID();

        // Любая операция (кроме create) над несуществующим атомиком должна бросать NOT_FOUND
        try {
            client
                    .atomicAdd(testKey,  null,10L).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // EXIST KEY OPERATIONS
    // =========================================================================

    @Test
    void testExistKeyExisting() throws ExecutionException, InterruptedException {
        String testKey = "existKeyExistingAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();
        Thread.sleep(150);
        Boolean exists = client
                .existKey(bytes(testKey))
                .get();
        assertTrue(exists, "Atomic key should exist");

        Thread.sleep(150);

        Boolean existsBackup = client
                .existKey(bytes(testKey))
                .get();
        assertTrue(existsBackup, "Atomic key should exist on backup");
    }

    @Test
    void testExistKeyNonExistent() throws ExecutionException, InterruptedException {
        String testKey = "existKeyNonExistAtomic" + UUID.randomUUID();

        Boolean exists = client
                .existKey(bytes(testKey))
                .get();
        assertFalse(exists, "Non-existent atomic key should not exist");
    }

    @Test
    void testExistKeyWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "existKeyWithCidAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 100L).get();
        Thread.sleep(150);
        Boolean exists = client
                .existKey(bytes(testKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT)
                .get();
        assertTrue(exists, "Atomic key should exist with clientId");
    }

    // =========================================================================
    // REMOVE OPERATIONS
    // =========================================================================

    @Test
    void testRemoveExistingAtomic() throws ExecutionException, InterruptedException {
        String testKey = "removeExistingAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();
        Thread.sleep(150);
        Boolean removed = client
                .remove(testKey)
                .get();
        assertTrue(removed, "Atomic key should be removed");

        Thread.sleep(150);

        // Verify the key is actually removed on master
        Boolean exists = client
                .existKey(bytes(testKey))
                .get();
        assertFalse(exists, "Removed key should not exist");

        // Verify on backup
        Boolean existsBackup = client
                .existKey(bytes(testKey))
                .get();
        assertFalse(existsBackup, "Removed key should not exist on backup");
    }

    @Test
    void testRemoveNonExistentAtomic() throws ExecutionException, InterruptedException {
        String testKey = "removeNonExistentAtomic" + UUID.randomUUID();

        try {
            client
                    .remove(testKey)
                    .get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // TTL ON ATOMIC KEY
    // =========================================================================

    @Test
    void testSetTtlOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "setTtlAtomic" + UUID.randomUUID();

        KeyHintData keyHint = client.atomicCreate(bytes(testKey), 42L).get();
        Thread.sleep(150);
        Boolean setResult = client
                .setTtl(testKey, keyHint, 300000)
                .get();
        assertTrue(setResult, "TTL should be set on atomic key");

        Long ttl = client
                .getTtl(testKey,keyHint)
                .get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL should be greater than 0");

        // Verify on backup (with replication delay)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client
                .getTtl(testKey,keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "TTL should be replicated to backup "+ttlBackup);
    }

    @Test
    void testTtlExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "ttlExpAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();
        Thread.sleep(150);
        // Set TTL = 1 second
        client
                .setTtl(testKey, null, 1)
                .get();

        // Wait for TTL expiration
        Thread.sleep(1500);

        // atomicLoad should return NOT_FOUND on master
        try {
            client
                    .atomicLoad(testKey)
                    .get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300);

        // Verify on backup
        try {
            client
                    .atomicLoad(testKey)
                    .get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status on backup");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // ATOMIC ADD EDGE CASES
    // =========================================================================

    @Test
    void testAtomicAddNegative() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddNegative" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 100L).get();
        Thread.sleep(150);
        // atomicAdd with negative delta → decrement
        client
                .atomicAdd(bytes(testKey), hint, -25L)
                .get();

        Thread.sleep(150);
        // Verify via atomicLoad
        long current = client
                .atomicLoad(testKey)
                .get();
        assertEquals(75L, current, "Value decreased by 25");

        // Verify on backup
        long currentBackup = client
                .atomicLoad(testKey)
                .get();
        assertEquals(75L, currentBackup);
    }

    @Test
    void testAtomicAddSaturation() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddSaturation" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), Long.MAX_VALUE).get();
        Thread.sleep(150);
        // Добавление 1 к Long.MAX_VALUE вернёт OUT_OF_RANGE
        try {
            client
                    .atomicAdd(bytes(testKey), hint, 1L)
                    .get();
            Assertions.fail("Expected ExecutionException caused by OUT_OF_RANGE status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.OUT_OF_RANGE, cause.getStatus().getCode());
        }

        // Значение не изменилось
        long current = client
                .atomicLoad(testKey,hint)
                .get();
        assertEquals(Long.MAX_VALUE, current, "Value unchanged at Long.MAX_VALUE");

        // Проверка репликации на бэкапе
        long currentBackup = client
                .atomicLoad(testKey,hint)
                .get();
        assertEquals(Long.MAX_VALUE, currentBackup);
    }

    @Test
    void testAtomicAddWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddWithCid" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 50L).get();
        Thread.sleep(150);
        client
                .atomicAdd(bytes(testKey), hint, 25L)
                .get();

        Thread.sleep(150);
        // Verify via atomicLoad
        long current = client
                .atomicLoad(testKey)
                .get();
        assertEquals(75L, current);

        // Verify on backup
        long currentBackup = client
                .atomicLoad(testKey)
                .get();
        assertEquals(75L, currentBackup);
    }

    @Test
    void testAtomicOrWithZeroMask() throws ExecutionException, InterruptedException {
        String testKey = "atomicOrZeroMask" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 100L).get();
        Thread.sleep(150);
        // atomicOr with mask 0L = read current value
        long current = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(100L, current);

        // Verify on backup
        long currentBackup = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(100L, currentBackup);
    }

    // =========================================================================
    // ATOMIC SUB EDGE CASES
    // =========================================================================

    @Test
    void testAtomicSubUnderflow() throws ExecutionException, InterruptedException {
        String testKey = "atomicSubUnderflow" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), Long.MIN_VALUE).get();
        Thread.sleep(150);
        // atomicSub from Long.MIN_VALUE → OUT_OF_RANGE
        try {
            client
                    .atomicSub(bytes(testKey), null, 1L)
                    .get();
            Assertions.fail("Expected ExecutionException caused by OUT_OF_RANGE status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.OUT_OF_RANGE, cause.getStatus().getCode());
        }

        // Значение не изменилось
        long current = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(Long.MIN_VALUE, current, "Value unchanged at Long.MIN_VALUE");

        // Verify on backup
        long currentBackup = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(Long.MIN_VALUE, currentBackup);
    }

    @Test
    void testAtomicSubWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "atomicSubWithCid" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 100L).get();
        Thread.sleep(150);
        long afterSub = client
                .atomicSub(bytes(testKey), null, 30L)
                .get();
        assertEquals(100L, afterSub);

        Thread.sleep(150);
        long current = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(70L, current);

        // Verify on backup
        long currentBackup = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(70L, currentBackup);
    }

    @Test
    void testAtomicOrWithNonZeroMask() throws ExecutionException, InterruptedException {
        String testKey = "atomicOrNonZero" + UUID.randomUUID();

        KeyHintData keyHint = client.atomicCreate(bytes(testKey), 8L).get();
        Thread.sleep(150);
        long afterOr = client
                .atomicOr(bytes(testKey), keyHint, 3L)
                .get();
        assertEquals(8L, afterOr, "Returns old value");

        long current = client
                .atomicOr(bytes(testKey), keyHint, 0L)
                .get();
        assertEquals(11L, current, "8 OR 3 = 11");
        Thread.sleep(150);
        // Verify on backup
        long currentBackup = client
                .atomicOr(bytes(testKey), keyHint, 0L)
                .get();
        assertEquals(11L, currentBackup);
    }

    // =========================================================================
    // CAS EDGE CASES
    // =========================================================================

    @Test
    void testCASWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "casWithCid" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 500L).get();
        Thread.sleep(150);
        AtomicCasRes res = client
                .atomicCompareAndSet(testKey, null, 500L, 600L, DEFAULT_CLIENT_ID)
                .get();
        assertTrue(res.getResult());

        Thread.sleep(150);
        long current = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(600L, current);

        // Verify on backup
        long currentBackup = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(600L, currentBackup);
    }

    @Test
    void testCASFailureReturnsActual() throws ExecutionException, InterruptedException {
        String testKey = "casFailureActual" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 500L).get();
        Thread.sleep(150);
        AtomicCasRes res = client
                .atomicCompareAndSet(testKey, null, 999L, 600L)
                .get();
        assertFalse(res.getResult());
        assertEquals(500L, res.getExpected().getVal(), "Returns actual value");

        Thread.sleep(150);
        long current = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(500L, current, "Value did not change");

        // Verify on backup
        long currentBackup = client
                .atomicOr(bytes(testKey), null, 0L)
                .get();
        assertEquals(500L, currentBackup);
    }

    // =========================================================================
    // LOCKING ON ATOMIC KEY
    // =========================================================================

    @Test
    void testReadLockOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "readLockAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();

        Thread.sleep(150);

        LockStatus lock1 = client
                .lockObject(testKey, LockType.READ_LOCK, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock1);

        LockStatus lock2 = client
                .lockObject(testKey, LockType.READ_LOCK, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock2);

        // Both can read
        long val1 = client
                .atomicLoad(testKey)
                .get();
        assertEquals(42L, val1);

        long val2 = client
                .atomicLoad(testKey)
                .get();
        assertEquals(42L, val2);

        // Verify on backup
        long valBackup = client
                .atomicLoad(testKey)
                .get();
        assertEquals(42L, valBackup);

        client.unlockObject(testKey).get();
        client.unlockObject(testKey).get();
    }

    @Test
    void testWriteLockOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "writeLockAtomic" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 42L).get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey,hint, LockType.WRITE_LOCK,OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        long val = client
                .atomicLoad(bytes(testKey),hint,OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(42L, val);

        // Owner can write
        long afterAdd = client
                .atomicAdd(bytes(testKey), hint, 10L,Duration.ofSeconds(30),OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(42L, afterAdd);

        // Intruder cannot read
        try {
            client
                    .atomicLoad(bytes(testKey),hint,INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                    .get();
            Assertions.fail("Intruder should be denied");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }
        Thread.sleep(150);
        // Verify on backup
        long valBackup = client
                .atomicLoad(bytes(testKey),hint,OWNER_CLIENT_ID,Duration.ofSeconds(30))
                .get();
        assertEquals(52L, valBackup);

        client.unlockObject(bytes(testKey),hint,OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
    }

    @Test
    void testGlobalLockOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "globalLockAtomic" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 42L)
                .get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        long val = client
                .atomicLoad(bytes(testKey), hint, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(42L, val);
        Thread.sleep(150);
        // Intruder cannot read
        try {
            client
                    .atomicLoad(bytes(testKey), hint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                    .get();
            Assertions.fail("Intruder should be denied");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot unlock
        LockStatus unlockStatus = client
                .unlockObject(testKey, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, unlockStatus);

        // Verify on backup
        long valBackup = client
                .atomicLoad(bytes(testKey),hint,OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(42L, valBackup);

        client.unlockObject(testKey,OWNER_CLIENT_ID).get();
    }

    @Test
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String testKey = "unlockOwnerAtomic" + UUID.randomUUID();

        KeyHintData keyHint = client.atomicCreate(bytes(testKey), 42L).get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey,keyHint, LockType.WRITE_LOCK,OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(150);
        // Intruder tries to unlock
        LockStatus status = client
                .unlockObject(testKey,keyHint,INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, status);

        // Owner unlocks
        LockStatus validUnlock = client
                .unlockObject(testKey,keyHint,OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, validUnlock);
    }

    @Test
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String testKey = "readLockBlocksWrite" + UUID.randomUUID();

        KeyHintData keyHint = client.atomicCreate(bytes(testKey), 42L).get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey,keyHint, LockType.READ_LOCK,OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);

        // Read OK
        long val = client
                .atomicLoad(bytes(testKey),keyHint,OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(42L, val);

        assertDenied(client
                             .atomicAdd(bytes(testKey), keyHint, 10L,Duration.ZERO,OWNER_CLIENT_ID,Duration.ofSeconds(30)));


        client.unlockObject(testKey,OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // LOCK EXPIRATION ON ATOMIC
    // =========================================================================

    @Test
    void testReadLockExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "readLockExpAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(3000);

        // Now intruder can read
        long val = client
                .atomicLoad(testKey)
                .get();
        assertEquals(42L, val);

        // Verify on backup
        long valBackup = client
                .atomicLoad(testKey)
                .get();
        assertEquals(42L, valBackup);
    }

    @Test
    void testWriteLockExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "writeLockExpAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client
                .lockObject(testKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(testKey).get();
    }

    @Test
    void testGlobalLockExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "globalLockExpAtomic" + UUID.randomUUID();

        KeyHintData hint = client.atomicCreate(bytes(testKey), 42L)
                .get();

        Thread.sleep(150);

        LockStatus lock = client
                .lockObject(testKey,hint, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(4000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client
                .lockObject(testKey,hint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(testKey,hint, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // ALREADY_EXISTS ERRORS
    // =========================================================================

    @Test
    void testAtomicCreateDuplicate() throws ExecutionException, InterruptedException {
        String testKey = "atomicCreateDup" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();
        Thread.sleep(150);
        try {
            client.atomicCreate(bytes(testKey), 100L)
                    .get();
            Assertions.fail("Expected ExecutionException caused by ALREADY_EXISTS status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.ALREADY_EXISTS, cause.getStatus().getCode());
        }
    }

    @Test
    void testCreateDifferentTypeOnAtomicKey() throws ExecutionException, InterruptedException {
        String testKey = "createTypeOnAtomic" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 42L).get();
        Thread.sleep(150);
        try {
            client
                    .createSet(testKey, new ArrayList<>())
                    .get();
            Assertions.fail("Expected ExecutionException caused by ALREADY_EXISTS status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.ALREADY_EXISTS, cause.getStatus().getCode());
        }
    }

    @Test
    void testCreateDifferentTypeOnSetKey() throws ExecutionException, InterruptedException {
        String testKey = "createTypeOnSet" + UUID.randomUUID();

        client.createSet(testKey, new ArrayList<>()).get();
        Thread.sleep(500);
        try {
            client.atomicCreate(bytes(testKey), 42L)
                    .get();
            Assertions.fail("Expected ExecutionException caused by ALREADY_EXISTS status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.ALREADY_EXISTS, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // ATOMIC LOAD AND DELETE
    // =========================================================================

    @Test
    void atomicLoadAndDeleteTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicLoadDeleteKey" + UUID.randomUUID();

        client.atomicCreate(bytes(testKey), 100L).get();
        Thread.sleep(150);

        long loadedValue = client
                .atomicLoadAndDelete(testKey)
                .get();
        Assertions.assertEquals(100L, loadedValue);

        Thread.sleep(150);
        try {
            client
                    .atomicLoad(testKey)
                    .get();
            Assertions.fail("Expected NOT_FOUND after delete");
        } catch (ExecutionException e) {
            Assertions.assertEquals(io.grpc.Status.Code.NOT_FOUND,
                ((io.grpc.StatusRuntimeException) e.getCause()).getStatus().getCode());
        }

        // Verify on backup
        try {
            client
                    .atomicLoad(testKey)
                    .get();
            Assertions.fail("Expected NOT_FOUND on backup after delete");
        } catch (ExecutionException e) {
            Assertions.assertEquals(io.grpc.Status.Code.NOT_FOUND,
                ((io.grpc.StatusRuntimeException) e.getCause()).getStatus().getCode());
        }
    }

}