package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RawValuesClusterTest extends TestBaseCluster {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final long REPLICATION_DELAY_MS = 100;

    // =========================================================================
    // 1. SET-TTL OPERATIONS (setTtl on master/backup, verify replication)
    // =========================================================================

    @Test
    @DisplayName("setTtl on master, verify TTL on both nodes")
    void testSetTtlOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_setTtl_master" + UUID.randomUUID();
        String testValue = "cluster_setTtl_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS); // 100ms replication wait after create

        // Verify key exists on both nodes before setTtl
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        // setTtl on MASTER (5000ms)
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(testKey, hint, 5000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        // Verify on MASTER: getTtl > 0
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(testKey, hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0 && ttlMaster <= 5000,
                "Expected TTL > 0 on master, got " + ttlMaster);

        // Verify on BACKUP: getTtl > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(testKey, hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0 && ttlBackup <= 5000,
                "Expected TTL > 0 on backup, got " + ttlBackup);
    }

    @Test
    @DisplayName("setTtl on backup, verify TTL on both nodes")
    void testSetTtlOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_setTtl_backup" + UUID.randomUUID();
        String testValue = "cluster_setTtl_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS); // 100ms replication wait after create

        // Verify key exists on both nodes before setTtl
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        // setTtl on BACKUP (5000ms)
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(testKey, hint, 5000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        // Verify on BACKUP: getTtl > 0
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(testKey, hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0 && ttlBackup <= 5000,
                "Expected TTL > 0 on backup, got " + ttlBackup);

        // Verify on MASTER: getTtl > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(testKey, hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0 && ttlMaster <= 5000,
                "Expected TTL > 0 on master, got " + ttlMaster);
    }

    // =========================================================================
    // 2. GET-AND-DELETE-VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteValue on master, verify deletion replicates to backup")
    void testGetAndDeleteValueOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_getDelete_master" + UUID.randomUUID();
        String testValue = "cluster_getDelete_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify key exists on both nodes before delete
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        // getAndDeleteValue on MASTER
        byte[] deletedValue = client.setMode(Mode.MASTER)
                .getAndDeleteValue(testKey, hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(deletedValue);
        assertEquals(testValue, new String(deletedValue, StandardCharsets.UTF_8));

        // Verify on MASTER: key is deleted (getAndDeleteValue returns NOT_FOUND)
        try {
            client.setMode(Mode.MASTER)
                    .getAndDeleteValue(testKey, hint, OWNER_CLIENT_ID)
                    .get();
            fail("Expected NOT_FOUND on master after getAndDeleteValue");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP: key is deleted (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getAndDeleteValue(testKey, hint, OWNER_CLIENT_ID)
                    .get();
            fail("Expected NOT_FOUND on backup after getAndDeleteValue replication");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("getAndDeleteValue on backup, verify deletion replicates to master")
    void testGetAndDeleteValueOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_getDelete_backup" + UUID.randomUUID();
        String testValue = "cluster_getDelete_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify key exists on both nodes before delete
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        // getAndDeleteValue on BACKUP
        byte[] deletedValue = client.setMode(Mode.BACKUP)
                .getAndDeleteValue(testKey, hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(deletedValue);
        assertEquals(testValue, new String(deletedValue, StandardCharsets.UTF_8));

        // Verify on BACKUP: key is deleted
        try {
            client.setMode(Mode.BACKUP)
                    .getAndDeleteValue(testKey, hint, OWNER_CLIENT_ID)
                    .get();
            fail("Expected NOT_FOUND on backup after getAndDeleteValue");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: key is deleted (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .getAndDeleteValue(testKey, hint, OWNER_CLIENT_ID)
                    .get();
            fail("Expected NOT_FOUND on master after getAndDeleteValue replication");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 3. UPDATE-KEY-VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("updateKeyValue on master, verify replication to backup")
    void testUpdateKeyValueOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_update_master" + UUID.randomUUID();
        String testValue = "cluster_update_value" + UUID.randomUUID();
        String testValueUpdate = "cluster_update_value_new" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify key exists on both nodes before update
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        // updateKeyValue on MASTER
        byte[] oldValue = client.setMode(Mode.MASTER)
                .updateKeyValue(bytes(testKey), hint, bytes(testValueUpdate))
                .get();
        assertNotNull(oldValue);
        assertEquals(testValue, new String(oldValue, StandardCharsets.UTF_8));

        // Verify on MASTER: getValue returns updated value
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterUpdated = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertEquals(testValueUpdate, new String(masterUpdated, StandardCharsets.UTF_8));

        // Verify on BACKUP: getValue returns updated value (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupUpdated = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertEquals(testValueUpdate, new String(backupUpdated, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateKeyValue on backup, verify replication to master")
    void testUpdateKeyValueOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_update_backup" + UUID.randomUUID();
        String testValue = "cluster_update_value" + UUID.randomUUID();
        String testValueUpdate = "cluster_update_value_new" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify key exists on both nodes before update
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        // updateKeyValue on BACKUP
        byte[] oldValue = client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(testKey), hint, bytes(testValueUpdate))
                .get();
        assertNotNull(oldValue);
        assertEquals(testValue, new String(oldValue, StandardCharsets.UTF_8));

        // Verify on BACKUP: getValue returns updated value
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupUpdated = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertEquals(testValueUpdate, new String(backupUpdated, StandardCharsets.UTF_8));

        // Verify on MASTER: getValue returns updated value (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterUpdated = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertEquals(testValueUpdate, new String(masterUpdated, StandardCharsets.UTF_8));
    }

    // =========================================================================
    // EXIST KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("existKey on master, verify key exists on both nodes")
    void testExistKeyOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_exist_master" + UUID.randomUUID();
        String testValue = "cluster_exist_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // existKey on MASTER
        Boolean existsMaster = client.setMode(Mode.MASTER)
                .existKey(bytes(testKey), hint)
                .get();
        assertTrue(existsMaster);

        // existKey on BACKUP (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean existsBackup = client.setMode(Mode.BACKUP)
                .existKey(bytes(testKey), hint)
                .get();
        assertTrue(existsBackup);
    }

    @Test
    @DisplayName("existKey on backup, verify key exists on both nodes")
    void testExistKeyOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_exist_backup" + UUID.randomUUID();
        String testValue = "cluster_exist_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // existKey on BACKUP
        Boolean existsBackup = client.setMode(Mode.BACKUP)
                .existKey(bytes(testKey), hint)
                .get();
        assertTrue(existsBackup);

        // existKey on MASTER (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean existsMaster = client.setMode(Mode.MASTER)
                .existKey(bytes(testKey), hint)
                .get();
        assertTrue(existsMaster);
    }

    // =========================================================================
    // 4. REMOVE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("remove on master, verify deletion replicates to backup")
    void testRemoveOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_remove_master" + UUID.randomUUID();
        String testValue = "cluster_remove_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify key exists on both nodes before remove
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        // remove on MASTER
        Boolean removed = client.setMode(Mode.MASTER)
                .remove(bytes(testKey), hint)
                .get();
        assertTrue(removed);

        // Verify on MASTER: key is removed (remove throws NOT_FOUND on second call)
        try {
            client.setMode(Mode.MASTER)
                    .remove(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on master after remove");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP: key is removed (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .remove(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on backup after remove replication");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("remove on backup, verify deletion replicates to master")
    void testRemoveOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_remove_backup" + UUID.randomUUID();
        String testValue = "cluster_remove_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify key exists on both nodes before remove
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(testKey, hint)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(testKey, hint)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        // remove on BACKUP
        Boolean removed = client.setMode(Mode.BACKUP)
                .remove(bytes(testKey), hint)
                .get();
        assertTrue(removed);

        // Verify on BACKUP: key is removed (remove throws NOT_FOUND on second call)
        try {
            client.setMode(Mode.BACKUP)
                    .remove(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on backup after remove");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: key is removed (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .remove(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on master after remove replication");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 5. TTL EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("TTL expiration: set short TTL on master, verify both nodes expire")
    void testSetTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_ttlExp_master" + UUID.randomUUID();
        String testValue = "cluster_ttlExp_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl = 2 seconds on MASTER
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(bytes(testKey), hint, 2000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: TTL > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        // Verify on BACKUP: TTL > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        // Wait for TTL to expire (2s + buffer)
        Thread.sleep(3000);

        // Verify on MASTER: key expired (NOT_FOUND)
        try {
            client.setMode(Mode.MASTER)
                    .getValue(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP: key expired (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getValue(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration: set short TTL on backup, verify both nodes expire")
    void testSetTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_ttlExp_backup" + UUID.randomUUID();
        String testValue = "cluster_ttlExp_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl = 2 seconds on BACKUP
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(bytes(testKey), hint, 2000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: TTL > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        // Verify on MASTER: TTL > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        // Wait for TTL to expire (2s + buffer)
        Thread.sleep(3000);

        // Verify on BACKUP: key expired (NOT_FOUND)
        try {
            client.setMode(Mode.BACKUP)
                    .getValue(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: key expired (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .getValue(bytes(testKey), hint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 6. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("LockObject on master, verify lock replicates to backup")
    void testLockObjectOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_lock_master" + UUID.randomUUID();
        String testValue = "cluster_lock_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(testKey), hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on MASTER: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID));

        // Unlock by owner
        LockStatus unlockStatus = client.setMode(Mode.MASTER)
                .unlockObject(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("LockObject on backup, verify lock replicates to master")
    void testLockObjectOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_lock_backup" + UUID.randomUUID();
        String testValue = "cluster_lock_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(testKey), hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on BACKUP: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        // Verify on MASTER: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID));

        // Unlock by owner
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    // =========================================================================
    // 7. LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("Lock expiration: set short TTL lock on master, verify both nodes expire")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String testKey = "cluster_lockExp_master" + UUID.randomUUID();
        String testValue = "cluster_lockExp_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK with 2s TTL on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(testKey), hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: owner can read (lock active)
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterValue);
        assertEquals(testValue, new String(masterValue, StandardCharsets.UTF_8));

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID));

        // Wait for lock TTL to expire (2s + buffer)
        Thread.sleep(3000);

        // Verify on MASTER: lock expired, intruder can read
        byte[] masterValueAfter = client.setMode(Mode.MASTER)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterValueAfter);
        assertEquals(testValue, new String(masterValueAfter, StandardCharsets.UTF_8));

        // Verify on BACKUP: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupValueAfter = client.setMode(Mode.BACKUP)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupValueAfter);
        assertEquals(testValue, new String(backupValueAfter, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes expire")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String testKey = "cluster_lockExp_backup" + UUID.randomUUID();
        String testValue = "cluster_lockExp_value" + UUID.randomUUID();

        // Create key-value, get KeyHintData
        KeyHintData hint = client.createKeyValue(testKey, bytes(testValue))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK with 2s TTL on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(testKey), hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: owner can read (lock active)
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(bytes(testKey), hint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupValue);
        assertEquals(testValue, new String(backupValue, StandardCharsets.UTF_8));

        // Verify on MASTER: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID));

        // Wait for lock TTL to expire (2s + buffer)
        Thread.sleep(3000);

        // Verify on BACKUP: lock expired, intruder can read
        byte[] backupValueAfter = client.setMode(Mode.BACKUP)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupValueAfter);
        assertEquals(testValue, new String(backupValueAfter, StandardCharsets.UTF_8));

        // Verify on MASTER: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterValueAfter = client.setMode(Mode.MASTER)
                .getValue(bytes(testKey), hint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterValueAfter);
        assertEquals(testValue, new String(masterValueAfter, StandardCharsets.UTF_8));
    }

    // ==================== Section 1: Create & Get Scalar Operations ====================

    @Test
    @DisplayName("Create scalar on master, verify replication to backup")
    void testCreateScalarOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalarKey" + UUID.randomUUID();
        String value = "scalarValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getValue returns correct value
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(key, keyHint)
                .get();
        assertNotNull(masterValue);
        assertEquals(value, new String(masterValue, StandardCharsets.UTF_8));

        // Verify on BACKUP: getValue returns correct value
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(key, keyHint)
                .get();
        assertNotNull(backupValue);
        assertEquals(value, new String(backupValue, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create scalar on backup, verify replication to master")
    void testCreateScalarOnBackup() throws ExecutionException, InterruptedException {
        String key = "scalarKeyBackup" + UUID.randomUUID();
        String value = "scalarValueBackup" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getValue returns correct value
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(key, keyHint)
                .get();
        assertNotNull(backupValue);
        assertEquals(value, new String(backupValue, StandardCharsets.UTF_8));

        // Verify on MASTER: getValue returns correct value
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(key, keyHint)
                .get();
        assertNotNull(masterValue);
        assertEquals(value, new String(masterValue, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Update scalar on master, verify replication to backup")
    void testUpdateScalarOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalarUpdateMaster" + UUID.randomUUID();
        String initialValue = "initialValue" + UUID.randomUUID();
        String updatedValue = "updatedValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(initialValue))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial value on BACKUP
        byte[] backupInitial = client.setMode(Mode.BACKUP)
                .getValue(key, keyHint)
                .get();
        assertEquals(initialValue, new String(backupInitial, StandardCharsets.UTF_8));

        // Update scalar on MASTER
        client.setMode(Mode.MASTER)
                .updateKeyValue(bytes(key), keyHint, bytes(updatedValue))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getValue returns updated value
        byte[] masterUpdated = client.setMode(Mode.MASTER)
                .getValue(key, keyHint)
                .get();
        assertEquals(updatedValue, new String(masterUpdated, StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getValue returns updated value
        byte[] backupUpdated = client.setMode(Mode.BACKUP)
                .getValue(key, keyHint)
                .get();
        assertEquals(updatedValue, new String(backupUpdated, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Update scalar on backup, verify replication to master")
    void testUpdateScalarOnBackup() throws ExecutionException, InterruptedException {
        String key = "scalarUpdateBackup" + UUID.randomUUID();
        String initialValue = "initialValue" + UUID.randomUUID();
        String updatedValue = "updatedValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(initialValue))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial value on MASTER
        byte[] masterInitial = client.setMode(Mode.MASTER)
                .getValue(key, keyHint)
                .get();
        assertEquals(initialValue, new String(masterInitial, StandardCharsets.UTF_8));

        // Update scalar on BACKUP
        client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), keyHint, bytes(updatedValue))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getValue returns updated value
        byte[] backupUpdated = client.setMode(Mode.BACKUP)
                .getValue(key, keyHint)
                .get();
        assertEquals(updatedValue, new String(backupUpdated, StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getValue returns updated value
        byte[] masterUpdated = client.setMode(Mode.MASTER)
                .getValue(key, keyHint)
                .get();
        assertEquals(updatedValue, new String(masterUpdated, StandardCharsets.UTF_8));
    }

    // ==================== Section 2: TTL Operations ====================

    @Test
    @DisplayName("SetTtl on master, verify GetTtl on both nodes")
    void testSetTtlAndGetTtlOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalarTtlMaster" + UUID.randomUUID();
        String value = "ttlValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Set TTL on BACKUP
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(bytes(key), keyHint, 300000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getTtl returns ~300000ms
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup >= 299000 && ttlBackup <= 300000,
                "Expected TTL ~300000ms on backup, got " + ttlBackup);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getTtl returns ~300000ms
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster >= 298000 && ttlMaster <= 300000,
                "Expected TTL ~300000ms on master, got " + ttlMaster);

        // Set TTL on MASTER
        Boolean ttlSet2 = client.setMode(Mode.MASTER)
                .setTtl(bytes(key), keyHint, 600000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet2);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getTtl returns ~600000ms
        Long ttlMaster2 = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster2);
        assertTrue(ttlMaster2 >= 599000 && ttlMaster2 <= 600000,
                "Expected TTL ~600000ms on master, got " + ttlMaster2);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getTtl returns ~600000ms
        Long ttlBackup2 = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup2);
        assertTrue(ttlBackup2 >= 598000 && ttlBackup2 <= 600000,
                "Expected TTL ~600000ms on backup, got " + ttlBackup2);
    }

    @Test
    @DisplayName("TTL expiration: set on master, verify both nodes after TTL expires")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalarTtlExpMaster" + UUID.randomUUID();
        String value = "ttlExpValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Set TTL = 2 seconds on MASTER
        client.setMode(Mode.MASTER)
                .setTtl(bytes(key), keyHint, 2000, OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on MASTER: getValue throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getValue(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getValue throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .getValue(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration: set on backup, verify both nodes after TTL expires")
    void testTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "scalarTtlExpBackup" + UUID.randomUUID();
        String value = "ttlExpValueBackup" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Set TTL = 2 seconds on BACKUP
        client.setMode(Mode.BACKUP)
                .setTtl(bytes(key), keyHint, 2000, OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on BACKUP: getValue throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .getValue(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getValue throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getValue(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // ==================== Section 3: Lock Operations ====================

    @Test
    @DisplayName("LockObject READ_LOCK: parallel reads OK, writes denied, unlock by owner only")
    void testLockObjectReadLock() throws ExecutionException, InterruptedException {
        String key = "scalarReadLock" + UUID.randomUUID();
        String value = "readLockValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on BACKUP: lockObject READ_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: client 1 can read (getValue)
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), keyHint, 1)
                .get();
        assertNotNull(backupValue);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: client 1 can read (getValue)
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(bytes(key), keyHint, 1)
                .get();
        assertNotNull(masterValue);

        // Write on BACKUP: client 2 can read (should succeed with READ_LOCK)
        byte[] backupValue2 = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), keyHint, 2)
                .get();
        assertNotNull(backupValue2);

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        assertDenied(client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), keyHint, "new_value".getBytes(StandardCharsets.UTF_8)));

        // Write on BACKUP: unlockObject by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS);
        // Write on MASTER: unlockObject by non-owner (client 999) should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner (client 1) should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("LockObject WRITE_LOCK: owner read/write OK, others denied, unlock by owner only")
    void testLockObjectWriteLock() throws ExecutionException, InterruptedException {
        String key = "scalarWriteLock" + UUID.randomUUID();
        String value = "writeLockValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on BACKUP: lockObject WRITE_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: client 1 can write (updateKeyValue)
        client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), keyHint, "updated_value".getBytes(StandardCharsets.UTF_8), 1)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: client 1 can read (getValue)
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(bytes(key), keyHint, 1)
                .get();
        assertNotNull(masterValue);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        assertDenied(client.setMode(Mode.BACKUP)
                .getValue(bytes(key), keyHint, 2));

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        assertDenied(client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), keyHint, "intruder_value".getBytes(StandardCharsets.UTF_8), 2));

        // Write on BACKUP: unlockObject by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS);
        // Write on MASTER: unlockObject by non-owner (client 2) should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, 2)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner (client 1) should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("LockObject GLOBAL: only owner any operations, others denied, unlock by owner only")
    void testLockObjectGlobalLock() throws ExecutionException, InterruptedException {
        String key = "scalarGlobalLock" + UUID.randomUUID();
        String value = "globalLockValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on BACKUP: lockObject GLOBAL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: client 1 can read (getValue)
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), keyHint, 1)
                .get();
        assertNotNull(backupValue);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: client 1 can read (getValue)
        byte[] masterValue = client.setMode(Mode.MASTER)
                .getValue(bytes(key), keyHint, 1)
                .get();
        assertNotNull(masterValue);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        assertDenied(client.setMode(Mode.BACKUP)
                .getValue(bytes(key), keyHint, 2));

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        assertDenied(client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), keyHint, "intruder_value".getBytes(StandardCharsets.UTF_8)));

        // Write on BACKUP: unlockObject by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS);
        // Write on MASTER: unlockObject by non-owner (client 2) should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, 2)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner (client 1) should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("UnlockObject: only lock owner can unlock, others get CANT_UNLOCK")
    void testUnlockObjectOwnerOnly() throws ExecutionException, InterruptedException {
        String key = "scalarUnlockOwner" + UUID.randomUUID();
        String value = "unlockOwnerValue" + UUID.randomUUID();

        // Create scalar, get KeyHintData
        KeyHintData keyHint = client.createKeyValue(key, bytes(value))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Lock by owner clientId=400
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, 400, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Try unlock by non-owner (clientId=999) should fail with CANT_UNLOCK
        LockStatus failedUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: lockObject by non-owner should still fail (lock still active)
        LockStatus cantLock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, 999, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.CANT_LOCK, cantLock);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Unlock by owner (clientId=400) should succeed
        LockStatus successUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, 400)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

}