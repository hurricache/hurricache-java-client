package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AtomicOperationsTest extends TestBase {

    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);


    @Test
    void atomicCreateAndStoreTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCreateStoreKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 42L).get());
        Thread.sleep(150);

        Assertions.assertNotNull(client.atomicStore(keyBytes, 100L).get());

        Thread.sleep(150);
        long current = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(100L, current);
    }

    @Test
    void atomicExchangeTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicExchangeKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 10L).get());
        Thread.sleep(150);

        long oldValue = client.atomicExchange(keyBytes, 20L).get();
        Assertions.assertEquals(10L, oldValue);

        Thread.sleep(150);
        long current = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(20L, current);
    }

    @Test
    void atomicAddAndSubTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddSubKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 50L).get());
        Thread.sleep(150);

        long afterAdd = client.atomicAdd(keyBytes, 25L).get();
        Assertions.assertEquals(50L, afterAdd);

        long afterSub = client.atomicSub(keyBytes, 10L).get();
        Assertions.assertEquals(75L, afterSub);

        Thread.sleep(150);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(65L, backupValue);
    }

    @Test
    void atomicBitwiseOpsTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicBitwiseKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 12L).get());
        Thread.sleep(150);

        long afterAnd = client.atomicAnd(keyBytes, null, 10L).get();
        Assertions.assertEquals(12L, afterAnd);

        long afterOr = client.atomicOr(keyBytes, null, 3L).get();
        Assertions.assertEquals(8L, afterOr);

        long afterXor = client.atomicXor(keyBytes, null, 15L).get();
        Assertions.assertEquals(11L, afterXor);

        Thread.sleep(150);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(4L, backupValue);
    }

    @Test
    void atomicCompareAndSetSuccessTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasSuccessKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 500L).get());
        Thread.sleep(150);

        AtomicCasRes res = client.atomicCompareAndSet(keyBytes, null, 500L, 600L).get();

        Assertions.assertTrue(res.getResult());
        Assertions.assertFalse(res.hasExpected());

        Thread.sleep(150);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(600L, backupValue);
    }

    @Test
    void atomicCompareAndSetFailureTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasFailKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 500L).get());
        Thread.sleep(150);

        AtomicCasRes res = client.atomicCompareAndSet(keyBytes, null, 999L, 600L).get();

        Assertions.assertFalse(res.getResult());
        Assertions.assertEquals(500L, res.getExpected().getVal());

        Thread.sleep(500);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(500L, backupValue);
    }

    @Test
    void atomicNonExistKeyTest() {
        String testKey = "atomicNonExistKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        try {
            client.atomicAdd(keyBytes, null, 10L).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 1. EXIST KEY OPERATIONS (fully uncovered)
    // =========================================================================

    @Test
    void testExistKeyExisting() throws ExecutionException, InterruptedException {
        String testKey = "existKeyExisting" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        Boolean exists = client.existKey(keyBytes).get();
        assertTrue(exists, "Atomic key should exist");
    }

    @Test
    void testExistKeyNonExistent() throws ExecutionException, InterruptedException {
        String testKey = "existKeyNonExist" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Boolean exists = client.existKey(keyBytes).get();
        assertFalse(exists, "Non-existent atomic key should not exist");
    }

    @Test
    void testExistKeyWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "existKeyWithCid" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 100L).get();

        Boolean exists = client.existKey(keyBytes, null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(exists, "Atomic key should exist with clientId");
    }

    // =========================================================================
    // 2. REMOVE OPERATIONS (success path)
    // =========================================================================

    @Test
    void testRemoveExistingAtomic() throws ExecutionException, InterruptedException {
        String testKey = "removeExistingAtomic" + UUID.randomUUID();

        client.atomicCreate(testKey, 42L).get();

        Boolean removed = client.remove(testKey).get();
        assertTrue(removed, "Atomic key should be removed");

        // Verify the key is actually removed
        Boolean exists = client.existKey(testKey).get();
        assertFalse(exists, "Removed key should not exist");
    }

    @Test
    void testRemoveNonExistentAtomic() throws ExecutionException, InterruptedException {
        String testKey = "removeNonExistentAtomic" + UUID.randomUUID();

        try {
            client.remove(testKey).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 3. TTL ON ATOMIC KEY
    // =========================================================================

    @Test
    void testSetTtlOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "setTtlAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        Boolean setResult = client.setTtl(testKey, null, 100).get();
        assertTrue(setResult, "TTL should be set on atomic key");

        Long ttl = client.getTtl(testKey).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL should be greater than 0");
    }

    @Test
    void testTtlExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "ttlExpAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        // Set TTL = 1 second
        client.setTtl(keyBytes, null, 1, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        // Wait for TTL expiration
        Thread.sleep(1500);

        // atomicLoad should return NOT_FOUND
        try {
            client.atomicLoad(testKey).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 4. ATOMIC ADD EDGE CASES
    // =========================================================================

    @Test
    void testAtomicAddNegative() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddNegative" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 100L).get();

        // atomicAdd with negative delta → decrement
        long afterAdd = client.atomicAdd(keyBytes, -25L).get();
        assertEquals(100L, afterAdd, "Returns old value");

        long current = client.atomicOr(keyBytes, null, 0L).get();
        assertEquals(75L, current, "Value decreased by 25");
    }

    @Test
    void testAtomicAddOverflow() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddOverflow" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, Long.MAX_VALUE).get();

        // atomicAdd on Long.MAX_VALUE → overflow
        long afterAdd = client.atomicAdd(keyBytes, 1L).get();
        assertEquals(Long.MAX_VALUE, afterAdd, "Returns old value");

        // After overflow value becomes Long.MIN_VALUE
        long current = client.atomicOr(keyBytes, null, 0L).get();
        assertEquals(Long.MIN_VALUE, current, "Overflow resulted in Long.MIN_VALUE");
    }

    @Test
    void testAtomicAddWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddWithCid" + UUID.randomUUID();

        client.atomicCreate(testKey, 50L).get();

        long afterAdd = client.atomicAdd(bytes(testKey), 25L).get();
        assertEquals(50L, afterAdd);

        long current = client.atomicOr(bytes(testKey), null, 0L).get();
        assertEquals(75L, current);
    }

    @Test
    void testAtomicOrWithZeroMask() throws ExecutionException, InterruptedException {
        String testKey = "atomicOrZeroMask" + UUID.randomUUID();

        client.atomicCreate(testKey, 100L).get();

        // atomicOr with mask 0L = read current value
        long current = client.atomicOr(bytes(testKey), null, 0L).get();
        assertEquals(100L, current);
    }

    // =========================================================================
    // 5. ATOMIC SUB EDGE CASES
    // =========================================================================

    @Test
    void testAtomicSubUnderflow() throws ExecutionException, InterruptedException {
        String testKey = "atomicSubUnderflow" + UUID.randomUUID();

        client.atomicCreate(testKey, 10L).get();

        // atomicSub > current value → negative result
        long afterSub = client.atomicSub(bytes(testKey), 50L).get();
        assertEquals(10L, afterSub, "Returns old value");

        long current = client.atomicOr(bytes(testKey), null, 0L).get();
        assertEquals(-40L, current, "Value is negative");
    }

    @Test
    void testAtomicSubWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "atomicSubWithCid" + UUID.randomUUID();

        client.atomicCreate(testKey, 100L).get();

        long afterSub = client.atomicSub(bytes(testKey), 30L).get();
        assertEquals(100L, afterSub);

        long current = client.atomicOr(bytes(testKey), null, 0L).get();
        assertEquals(70L, current);
    }

    @Test
    void testAtomicOrWithNonZeroMask() throws ExecutionException, InterruptedException {
        String testKey = "atomicOrNonZero" + UUID.randomUUID();

        client.atomicCreate(testKey, 8L).get();

        long afterOr = client.atomicOr(bytes(testKey), null, 3L).get();
        assertEquals(8L, afterOr, "Returns old value");

        long current = client.atomicOr(bytes(testKey), null, 0L).get();
        assertEquals(11L, current, "8 OR 3 = 11");
    }

    // =========================================================================
    // 6. CAS EDGE CASES
    // =========================================================================

    @Test
    void testCASWithClientId() throws ExecutionException, InterruptedException {
        String testKey = "casWithCid" + UUID.randomUUID();

        client.atomicCreate(testKey, 500L).get();

        AtomicCasRes res = client.atomicCompareAndSet(testKey, null, 500L, 600L, DEFAULT_CLIENT_ID).get();
        assertTrue(res.getResult());

        long current = client.atomicOr(bytes(testKey), null, 0L).get();
        assertEquals(600L, current);
    }

    @Test
    void testCASFailureReturnsActual() throws ExecutionException, InterruptedException {
        String testKey = "casFailureActual" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 500L).get();

        AtomicCasRes res = client.atomicCompareAndSet(keyBytes, null, 999L, 600L).get();
        assertFalse(res.getResult());
        assertEquals(500L, res.getExpected().getVal(), "Returns actual value");

        long current = client.atomicOr(keyBytes, null, 0L).get();
        assertEquals(500L, current, "Value did not change");
    }

    // =========================================================================
    // 7. LOCKING ON ATOMIC KEY
    // =========================================================================

    @Test
    void testReadLockOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "readLockAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock1 = client.lockObject(testKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1);

        LockStatus lock2 = client.lockObject(testKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2);

        // Both can read
        long val1 = client.atomicLoad(testKey, DEFAULT_CLIENT_ID).get();
        assertEquals(42L, val1);

        long val2 = client.atomicLoad(testKey, INTRUDER_CLIENT_ID).get();
        assertEquals(42L, val2);

        client.unlockObject(testKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    void testWriteLockOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "writeLockAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        long val = client.atomicLoad(testKey, OWNER_CLIENT_ID).get();
        assertEquals(42L, val);

        // Owner can write
        long afterAdd = client.atomicAdd(bytes(testKey),null, 10L,null,OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(42L, afterAdd);

        assertDenied(client.atomicLoad(testKey, INTRUDER_CLIENT_ID));

        client.unlockObject(testKey, OWNER_CLIENT_ID).get();
    }

    @Test
    void testGlobalLockOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "globalLockAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        long val = client.atomicLoad(testKey, OWNER_CLIENT_ID).get();
        assertEquals(42L, val);

        // Intruder cannot read
        try {
            client.atomicLoad(testKey, INTRUDER_CLIENT_ID).get();
            Assertions.fail("Intruder should be denied");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot unlock
        LockStatus unlockStatus = client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlockStatus);

        client.unlockObject(testKey, OWNER_CLIENT_ID).get();
    }

    @Test
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String testKey = "unlockOwnerAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder tries to unlock
        LockStatus status = client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, status);

        // Owner unlocks
        LockStatus validUnlock = client.unlockObject(testKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock);
    }

    @Test
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String testKey = "readLockBlocksWrite" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Read OK
        long val = client.atomicLoad(testKey, DEFAULT_CLIENT_ID).get();
        assertEquals(42L, val);

        // Write is denied
        try {
            client.atomicAdd(bytes(testKey), 10L).get();
            Assertions.fail("Write should be denied under READ_LOCK");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        client.unlockObject(testKey, DEFAULT_CLIENT_ID).get();
    }

    // =========================================================================
    // 8. LOCK EXPIRATION ON ATOMIC
    // =========================================================================

    @Test
    void testReadLockExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "readLockExpAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(3000);

        // Now intruder can read
        long val = client.atomicLoad(testKey, INTRUDER_CLIENT_ID).get();
        assertEquals(42L, val);
    }

    @Test
    void testWriteLockExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "writeLockExpAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client.lockObject(testKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    void testGlobalLockExpirationOnAtomic() throws ExecutionException, InterruptedException {
        String testKey = "globalLockExpAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        LockStatus lock = client.lockObject(testKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client.lockObject(testKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(testKey, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // 9. ALREADY_EXISTS ERRORS
    // =========================================================================

    @Test
    void testAtomicCreateDuplicate() throws ExecutionException, InterruptedException {
        String testKey = "atomicCreateDup" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        try {
            client.atomicCreate(keyBytes, 100L).get();
            Assertions.fail("Expected ExecutionException caused by ALREADY_EXISTS status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.ALREADY_EXISTS, cause.getStatus().getCode());
        }
    }

    @Test
    void testCreateDifferentTypeOnAtomicKey() throws ExecutionException, InterruptedException {
        String testKey = "createTypeOnAtomic" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 42L).get();

        try {
            client.createSet(testKey, new ArrayList<>()).get();
            Assertions.fail("Expected ExecutionException caused by ALREADY_EXISTS status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.ALREADY_EXISTS, cause.getStatus().getCode());
        }
    }

    @Test
    void testCreateDifferentTypeOnSetKey() throws ExecutionException, InterruptedException {
        String testKey = "createTypeOnSet" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.createSet(testKey, new ArrayList<>()).get();

        try {
            client.atomicCreate(keyBytes, 42L).get();
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

        client.atomicCreate(testKey, 100L).get();
        Thread.sleep(150);

        long loadedValue = client.atomicLoadAndDelete(testKey).get();
        Assertions.assertEquals(100L, loadedValue);

        Thread.sleep(150);
        try {
            client.atomicLoad(testKey).get();
            Assertions.fail("Expected NOT_FOUND after delete");
        } catch (ExecutionException e) {
            Assertions.assertEquals(io.grpc.Status.Code.NOT_FOUND,
                ((io.grpc.StatusRuntimeException) e.getCause()).getStatus().getCode());
        }
    }

    // =========================================================================
    // SHUTDOWN
    // =========================================================================

    @Test
    void shutdownTest() throws ExecutionException, InterruptedException {
        String testKey = "shutdownTestKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 99L).get();
        Thread.sleep(150);

        long value = client.atomicLoad(testKey).get();
        Assertions.assertEquals(99L, value);

        client.shutdown();
    }
}