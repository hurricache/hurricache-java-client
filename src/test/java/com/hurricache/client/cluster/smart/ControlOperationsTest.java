package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ControlOperationsTest extends TestBaseCluster {

    @Test
    void testTtlMethodsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttlKey" + UUID.randomUUID();

        // Create should be done on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(key, "data".getBytes())
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        ;

        // All other access should be done on backup
        Boolean success = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).setTtl(key, keyHint, 100).get();
        Assertions.assertTrue(success);

        // Verify TTL
        Long res = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getTtl(key, keyHint).get();
        Assertions.assertTrue(res > 0 && res <= 100, () -> res.toString() + " " + key);
    }

    @Test
    void testTtlMethodsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlKey" + UUID.randomUUID();

        // Create should be done on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(key, "data".getBytes())
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // All other access should be done on master
        Boolean success = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).setTtl(key, keyHint, 100).get();
        Assertions.assertTrue(success);

        // Verify TTL
        Long res = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getTtl(key,keyHint).get();
        Assertions.assertTrue(res > 0 && res <= 100, () -> res + " " + key);
    }

    @Test
    void testLockingMechanismCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String lockKey = "resourceKey" + UUID.randomUUID();

        // Create should be done on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey, "secure_data".getBytes())
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // All other access should be done on backup
        // Client 1 acquires lock
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey, keyHint, LockType.WRITE_LOCK, 101, Duration.ofSeconds(30)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // Client 2 attempts to lock (should fail based on server logic)
        LockStatus lockResConflict = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey, keyHint, LockType.WRITE_LOCK, 102, Duration.ofSeconds(30)).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict);
    }

    @Test
    void testLockingMechanismCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String lockKey = "resourceKey" + UUID.randomUUID();

        // Create should be done on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey, "secure_data".getBytes())
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        ;

        // All other access should be done on master
        // Client 1 acquires lock
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey, keyHint, LockType.WRITE_LOCK, 101, Duration.ofSeconds(30)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // Client 2 attempts to lock (should fail based on server logic)
        LockStatus lockResConflict = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey, keyHint, LockType.WRITE_LOCK, 102, Duration.ofSeconds(30)).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict);
    }
}