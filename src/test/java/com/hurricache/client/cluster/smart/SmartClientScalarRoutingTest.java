package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Scalar operations.
 * Mirrors ScalarOperationsTest for cluster routing.
 */
public class SmartClientScalarRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + READ SCALAR WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create scalar with bytes, read on MASTER then BACKUP")
    void testCreateReadBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "scalar_bytes_" + UUID.randomUUID();
        String value = "hello_world";

        // 1. Create - NO setMode needed
        KeyHintData hint = client.create(key, v(value)).get();
        assertNotNull(hint);

        // 2. Pause for replication
        Thread.sleep(500);

        // 3. Read on MASTER
        Variant result = client.setMode(Mode.MASTER).get(key, hint).get();
        assertNotNull(result);
        assertEquals("hello_world", new String(result.bytesValue(), StandardCharsets.UTF_8));

        // 4. Read on BACKUP
        Variant backupResult = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(backupResult);
        assertEquals("hello_world", new String(backupResult.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create scalar with int, read on MASTER then BACKUP")
    void testCreateReadIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "scalar_int_" + UUID.randomUUID();
        int value = 42;

        KeyHintData hint = client.create(key, vi(value)).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant result = client.setMode(Mode.MASTER).get(key, hint).get();
        assertNotNull(result);
        assertEquals(42, result.longValue());

        Variant backupResult = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(backupResult);
        assertEquals(42, backupResult.longValue());
    }

    @Test
    @DisplayName("Create scalar with double, read on MASTER then BACKUP")
    void testCreateReadDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "scalar_double_" + UUID.randomUUID();
        double value = 3.14;

        KeyHintData hint = client.create(key, vd(value)).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant result = client.setMode(Mode.MASTER).get(key, hint).get();
        assertNotNull(result);
        assertEquals(3.14, result.doubleValue(), 0.001);

        Variant backupResult = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(backupResult);
        assertEquals(3.14, backupResult.doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. TYPE SWITCHING WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Replace bytes with int, validate on MASTER and BACKUP")
    void testSwitchBytesToIntOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "switch_bytes_to_int_" + UUID.randomUUID();

        // Create with bytes
        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);
        Variant v1 = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals("hello", new String(v1.bytesValue(), StandardCharsets.UTF_8));

        // Update to int on MASTER
        client.setMode(Mode.MASTER);
        client.replace(key, hint, vi(42)).get();
        Thread.sleep(500);

        // Verify on BACKUP
        Variant v2 = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(42, v2.longValue());
    }

    @Test
    @DisplayName("Replace int with double, validate on MASTER and BACKUP")
    void testSwitchIntToDoubleOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "switch_int_to_double_" + UUID.randomUUID();

        // Create with int
        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // Update to double on MASTER
        client.setMode(Mode.MASTER);
        client.replace(key, hint, vd(3.14)).get();
        Thread.sleep(500);

        // Verify on BACKUP
        Variant v2 = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(3.14, v2.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Replace double with bytes, validate on MASTER and BACKUP")
    void testSwitchDoubleToBytesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "switch_double_to_bytes_" + UUID.randomUUID();

        // Create with double
        Variant vd = vd(2.5);
        KeyHintData hint = client.create(key, vd).get();
        Thread.sleep(500);
        Variant v1 = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals(2.5, v1.doubleValue(), 0.001);

        // Update to bytes on MASTER
        client.setMode(Mode.MASTER);
        Variant variant = client.replace(key, hint, v("new_value")).get();
        assertEquals(vd, variant);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant v2 = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals("new_value", new String(v2.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. GET AND DELETE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteValue on MASTER, verify removed on BACKUP")
    void testGetAndDeleteValueOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "get_delete_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // GetAndDelete on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.pop(key, hint).get();
        assertNotNull(deleted);
        assertEquals(100, deleted.longValue());
        Thread.sleep(500);

        // Verify on BACKUP - should not exist
        Boolean backupValue = client.setMode(Mode.BACKUP).containsKey(key, hint).get();
        assertFalse(backupValue);
    }

    // =========================================================================
    // 4. TTL OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("setTtl and getTtl on MASTER, validate on BACKUP")
    void testSetTtlAndGetTtlOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttl_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(42)).get();
        Thread.sleep(500);

        // SetTtl on MASTER
        client.setMode(Mode.MASTER);
        Boolean setResult = client.setTtl(key, hint, 5000).get();
        assertTrue(setResult);
        Thread.sleep(500);

        // Verify on BACKUP
        Long ttl = client.setMode(Mode.BACKUP).getTtl(key, hint).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 5000);
    }

    @Test
    @DisplayName("TTL expiration on scalar with routing")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttl_expire_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(42)).get();
        Thread.sleep(500);

        // Set very short TTL on MASTER
        client.setMode(Mode.MASTER);
        client.setTtl(key, hint, 1).get();
        Thread.sleep(2000);

        // Verify on BACKUP - should not exist
        Boolean backupValue = client.setMode(Mode.BACKUP).containsKey(key, hint).get();
        assertFalse(backupValue);
    }

    // =========================================================================
    // 5. EXIST + REMOVE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("existKey and remove on MASTER, validate on BACKUP")
    void testExistAndRemoveOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "exist_remove_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("test")).get();
        Thread.sleep(500);

        // ExistKey on MASTER
        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, hint).get();
        assertTrue(exists);

        // Remove on MASTER
        client.setMode(Mode.MASTER);
        Boolean removed = client.remove(key, hint).get();
        assertTrue(removed);
        Thread.sleep(500);

        // Verify on BACKUP - should not exist
        exists = client.setMode(Mode.BACKUP).containsKey(key, hint).get();
        assertFalse(exists);
    }

    // =========================================================================
    // 6. LOCK OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnMaster() throws ExecutionException, InterruptedException {
        String key = "read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP (shared access)
        Variant val = client.setMode(Mode.BACKUP).get(key, hint, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        Variant intruderVal = client.setMode(Mode.BACKUP).get(key, hint, 999).get();
        assertNotNull(intruderVal);

        // Neither can write on BACKUP
        try {
            client.setMode(Mode.BACKUP).replace(key, hint, v("updated"), 100).get();
            fail("Expected exception for write with READ_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnMaster() throws ExecutionException, InterruptedException {
        String key = "write_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Variant val = client.setMode(Mode.BACKUP).get(key, hint, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.replace(key, hint, v("updated"), 100).get();
        Thread.sleep(500);

        // Verify on BACKUP - should be updated
        val = client.setMode(Mode.BACKUP).get(key, hint, 100).get();
        assertEquals("updated", new String(val.bytesValue(), StandardCharsets.UTF_8));

        // Intruder denied on BACKUP
        try {
            client.setMode(Mode.BACKUP).get(key, hint, 1001).get();
            fail("Expected exception for intruder read with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on MASTER: only owner can access")
    void testGlobalLockOnMaster() throws ExecutionException, InterruptedException {
        String key = "global_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Variant val = client.setMode(Mode.BACKUP).get(key, hint, 100).get();
        assertNotNull(val);

        // Intruder denied on BACKUP
        try {
            client.setMode(Mode.BACKUP).get(key, hint, 1001).get();
            fail("Expected exception for intruder read with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Unlock by owner only on MASTER")
    void testUnlockOwnerOnlyOnMaster() throws ExecutionException, InterruptedException {
        String key = "unlock_owner_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Intruder cannot unlock on BACKUP
        LockStatus unlock = client.setMode(Mode.BACKUP).unlock(key, hint, 200).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        // Owner unlocks on MASTER
        unlock = client.setMode(Mode.MASTER).unlock(key, hint, 100).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Lock expiration on scalar with routing")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "lock_expire_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, intruder can access on BACKUP
        Variant val = client.setMode(Mode.BACKUP).get(key, hint, 100).get();
        assertNotNull(val);
    }

    // =========================================================================
    // 7. SIZE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getSize on MASTER then BACKUP with routing")
    void testGetSizeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "scalar_size_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("test_value")).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertNotNull(size);
        assertTrue(size > 0);

        Integer backupSize = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(size, backupSize);
    }
}