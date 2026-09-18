package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Cluster tests for lock permission model across MASTER and BACKUP nodes.
 * Each test verifies that locks acquired on one node properly block/unblock operations on both nodes.
 */
public class LockMethodProtectionTest extends TestBaseCluster {

    private static final int OWNER_ID = 100;
    private static final int INTRUDER_ID = 200;
    private static final long REPLICATION_DELAY_MS = 300;

    // =========================================================================
    // Section 1: GLOBAL LOCK - UNARY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("GLOBAL Lock: Blocks all operations from intruder - create on Master")
    void testGlobalLockUnaryProtectionCreateOnMaster() throws Exception {
        String key = "lock_global_unary_master_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Lock on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.GLOBAL, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Verify intruder blocked on MASTER (immediate)
        assertDenied(client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.MASTER)
                .updateKeyValue(bytes(key), hint, "new".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(30), INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.MASTER)
                .remove(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));

        // Replication delay → verify intruder blocked on BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), hint, "new".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(30), INTRUDER_ID, Duration.ofSeconds(2)));

        // Unlock by owner
        client.setMode(Mode.MASTER).unlockObject(key, hint, OWNER_ID).get();
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks all operations from intruder - create on Backup")
    void testGlobalLockUnaryProtectionCreateOnBackup() throws Exception {
        String key = "lock_global_unary_backup_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Lock on BACKUP
        LockStatus lock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.GLOBAL, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Verify intruder blocked on BACKUP (immediate)
        assertDenied(client.setMode(Mode.BACKUP)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), hint, "new".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(30), INTRUDER_ID, Duration.ofSeconds(2)));

        // Replication delay → verify intruder blocked on MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.MASTER)
                .updateKeyValue(bytes(key), hint, "new".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(30), INTRUDER_ID, Duration.ofSeconds(2)));

        // Unlock by owner
        client.setMode(Mode.BACKUP).unlockObject(key, hint, OWNER_ID).get();
    }

    // =========================================================================
    // Section 2: GLOBAL LOCK - COLLECTION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("GLOBAL Lock: Blocks collection operations from intruder - create on Master")
    void testGlobalLockCollectionProtectionCreateOnMaster() throws Exception {
        String key = "lock_global_collection_master_" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of("item1".getBytes(StandardCharsets.UTF_8)));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, initialData).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Lock on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.GLOBAL, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Verify intruder blocked on MASTER (immediate)
        assertDenied(client.setMode(Mode.MASTER)
                .getHead(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.MASTER)
                .addElementToTail(bytes(key), hint, List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8))), INTRUDER_ID, Duration.ofSeconds(2)));

        // Replication delay → verify intruder blocked on BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getHead(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.BACKUP)
                .addElementToTail(bytes(key), hint, List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8))), INTRUDER_ID, Duration.ofSeconds(2)));

        // Unlock by owner
        client.setMode(Mode.MASTER).unlockObject(key, hint, OWNER_ID).get();
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks collection operations from intruder - create on Backup")
    void testGlobalLockCollectionProtectionCreateOnBackup() throws Exception {
        String key = "lock_global_collection_backup_" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of("item1".getBytes(StandardCharsets.UTF_8)));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, initialData).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Lock on BACKUP
        LockStatus lock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.GLOBAL, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Verify intruder blocked on BACKUP (immediate)
        assertDenied(client.setMode(Mode.BACKUP)
                .getHead(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.BACKUP)
                .addElementToTail(bytes(key), hint, List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8))), INTRUDER_ID, Duration.ofSeconds(2)));

        // Replication delay → verify intruder blocked on MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getHead(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)));
        assertDenied(client.setMode(Mode.MASTER)
                .addElementToTail(bytes(key), hint, List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8))), INTRUDER_ID, Duration.ofSeconds(2)));

        // Unlock by owner
        client.setMode(Mode.BACKUP).unlockObject(key, hint, OWNER_ID).get();
    }

    // =========================================================================
    // Section 3: READ LOCK
    // =========================================================================

    @Test
    @DisplayName("READ Lock: Allows reads, blocks writes - create on Master")
    void testReadLockProtectionCreateOnMaster() throws Exception {
        String key = "lock_read_master_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Lock on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.READ_LOCK, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read on MASTER (immediate)
        byte[] val = client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, OWNER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(val);

        // Replication delay → verify owner can read on BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        byte[] backupVal = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), hint, OWNER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(backupVal);

        // Write is denied on MASTER
        assertDenied(client.setMode(Mode.MASTER)
                .updateKeyValue(bytes(key), hint, "bad".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(30), OWNER_ID, Duration.ofSeconds(2)));

        // Unlock by owner
        client.setMode(Mode.MASTER).unlockObject(key, hint, OWNER_ID).get();
    }

    @Test
    @DisplayName("READ Lock: Allows reads, blocks writes - create on Backup")
    void testReadLockProtectionCreateOnBackup() throws Exception {
        String key = "lock_read_backup_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Lock on BACKUP
        LockStatus lock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.READ_LOCK, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read on BACKUP (immediate)
        byte[] val = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), hint, OWNER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(val);

        // Replication delay → verify owner can read on MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        byte[] masterVal = client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, OWNER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(masterVal);

        // Write is denied on BACKUP
        assertDenied(client.setMode(Mode.BACKUP)
                .updateKeyValue(bytes(key), hint, "bad".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(30), OWNER_ID, Duration.ofSeconds(2)));

        // Unlock by owner
        client.setMode(Mode.BACKUP).unlockObject(key, hint, OWNER_ID).get();
    }

    // =========================================================================
    // Section 4: LOCK COMPATIBILITY
    // =========================================================================

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE_LOCK if READ_LOCK exists - create on Master")
    void testLockCompatibilityCreateOnMaster() throws Exception {
        String key = "lock_compat_master_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Acquire READ_LOCK on MASTER
        LockStatus readLock = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.READ_LOCK, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, readLock);

        // WRITE_LOCK should fail with CANT_LOCK on MASTER
        LockStatus writeLock = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.WRITE_LOCK, INTRUDER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, writeLock);

        // READ_LOCK should succeed for intruder on MASTER
        LockStatus readLock2 = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.READ_LOCK, INTRUDER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, readLock2);

        // Replication delay → verify on BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        byte[] backupVal = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(backupVal);

        // Unlock both
        client.setMode(Mode.MASTER).unlockObject(key, hint, OWNER_ID).get();
        client.setMode(Mode.MASTER).unlockObject(key, hint, INTRUDER_ID).get();
    }

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE_LOCK if READ_LOCK exists - create on Backup")
    void testLockCompatibilityCreateOnBackup() throws Exception {
        String key = "lock_compat_backup_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Acquire READ_LOCK on BACKUP
        LockStatus readLock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.READ_LOCK, OWNER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, readLock);
        Thread.sleep((int) REPLICATION_DELAY_MS);
        // WRITE_LOCK should fail with CANT_LOCK on BACKUP
        LockStatus writeLock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.WRITE_LOCK, INTRUDER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, writeLock);

        // READ_LOCK should succeed for intruder on BACKUP
        LockStatus readLock2 = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.READ_LOCK, INTRUDER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, readLock2);

        // Replication delay → verify on MASTER

        byte[] masterVal = client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(masterVal);

        // Unlock both
        client.setMode(Mode.BACKUP).unlockObject(key, hint, OWNER_ID).get();
        client.setMode(Mode.BACKUP).unlockObject(key, hint, INTRUDER_ID).get();
    }

    // =========================================================================
    // Section 5: LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("READ Lock expiration: 2s lock expires, intruder can read on Master and Backup")
    void testReadLockExpirationOnMaster() throws Exception {
        String key = "lock_read_exp_master_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Acquire READ_LOCK with 2s TTL on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(key, hint, LockType.READ_LOCK, OWNER_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for lock expiration
        Thread.sleep(3000);

        // Now intruder can read on MASTER
        byte[] val = client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(val);

        // Replication delay → verify on BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        byte[] backupVal = client.setMode(Mode.BACKUP)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(backupVal);
    }

    @Test
    @DisplayName("WRITE Lock expiration: 2s lock expires, intruder can acquire WRITE_LOCK on Backup and Master")
    void testWriteLockExpirationOnBackup() throws Exception {
        String key = "lock_write_exp_backup_" + UUID.randomUUID();

        // Create WITHOUT setMode
        KeyHintData hint = client.createKeyValue(key, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        assertNotNull(hint);

        // Small object replication wait
        Thread.sleep(500);

        // Acquire WRITE_LOCK with 2s TTL on BACKUP
        LockStatus lock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.WRITE_LOCK, OWNER_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for lock expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK on BACKUP
        LockStatus newLock = client.setMode(Mode.BACKUP)
                .lockObject(key, hint, LockType.WRITE_LOCK, INTRUDER_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        // Replication delay → verify on MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        byte[] masterVal = client.setMode(Mode.MASTER)
                .getValue(bytes(key), hint, INTRUDER_ID, Duration.ofSeconds(2)).get();
        assertNotNull(masterVal);

        // Unlock
        client.setMode(Mode.BACKUP).unlockObject(key, hint, INTRUDER_ID).get();
    }
}