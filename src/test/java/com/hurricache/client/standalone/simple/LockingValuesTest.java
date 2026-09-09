package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
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
import java.util.concurrent.TimeUnit;

public class LockingValuesTest extends TestBase {

    @Test
    void testUnanimousLockAndAnyUnlock() throws ExecutionException, InterruptedException {
        String lockKey1 = "existing_lock_object" + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(lockKey1, "initial_data".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(lockKey1, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes, "Should lock unanimously");

        LockStatus unlockRes = client.unlockObject(lockKey1, 999).get();
        Assertions.assertEquals(LockStatus.OK, unlockRes, "Any client should be able to unlock a unanimous lock");
    }

    @Test
    void testSpecificLockAndRestrictedUnlock() throws ExecutionException, InterruptedException {
        int ownerId = 100;
        int intruderId = 200;

        String lockKey1 = "existing_lock_object" + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(lockKey1, "initial_data".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(lockKey1, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        LockStatus failedUnlock = client.unlockObject(lockKey1, intruderId).get();
        Assertions.assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        LockStatus successUnlock = client.unlockObject(lockKey1, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    void testLockingConflict() throws ExecutionException, InterruptedException {
        String lockKey1 = "existing_lock_object" + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(lockKey1, "initial_data".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(500);

        client.lockObject(lockKey1, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();

        LockStatus conflictRes = client.lockObject(lockKey1, LockType.WRITE_LOCK, 2, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes, "Should not allow double locking");
    }

    @Test
    void testUnanimousLockBlocksSpecificLock() throws ExecutionException, InterruptedException {
        String lockKey1 = "existing_lock_object" + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(lockKey1, "initial_data".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(500);

        client.lockObject(lockKey1, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        LockStatus conflictRes = client.lockObject(lockKey1, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes);
    }

    @Test
    void testUnlockOnExpiredObject() throws ExecutionException, InterruptedException {
        String lockKey1 = "existing_lock_object" + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(lockKey1, "initial_data".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(500);

        LockStatus lockStatus = client.lockObject(lockKey1, LockType.WRITE_LOCK, 555, Duration.ofSeconds(1)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        LockStatus res = client.unlockObject(lockKey1, 999).get();
        Assertions.assertEquals(LockStatus.OK, res, "Unlock on expired lock should return OK");
    }

    @Test
    void testGlobalLockBlocksDataAccess() throws ExecutionException, InterruptedException {
        String key = "global_data_key" + UUID.randomUUID();
        int ownerId = 1;
        int intruderId = 2;

        Assertions.assertNotNull(client.createKeyValue(key, "sensitive_info".getBytes()).get());
        Thread.sleep(500);

        client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();

        byte[] data = client.getValue(key, ownerId).get();
        Assertions.assertNotNull(data);

        try {
            client.getValue(key, intruderId).get();
            Assertions.fail("Intruder should have been blocked by GLOBAL lock");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }
    }
}