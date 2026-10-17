package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Scalar operations WITHOUT keyHint.
 * Tests default routing behavior when keyHint is null.
 */
public class SmartClientScalarNoHintRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + READ SCALAR WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("Create scalar with bytes, read on MASTER without keyHint")
    void testCreateReadBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalar_no_hint_bytes_" + UUID.randomUUID();
        String value = "hello_world";

        // Create returns keyHint but we won't use it
        KeyHintData hint = client.create(key, v(value)).get();

        // Read on MASTER without keyHint
        Thread.sleep(500);
        Variant result = client.setMode(Mode.MASTER).get(key).get();
        assertNotNull(result);
        assertEquals("hello_world", new String(result.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create scalar with int, read on MASTER without keyHint")
    void testCreateReadIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalar_no_hint_int_" + UUID.randomUUID();
        int value = 42;

        KeyHintData hint = client.create(key, vi(value)).get();
        Thread.sleep(500);

        Variant result = client.setMode(Mode.MASTER).get(key).get();
        assertNotNull(result);
        assertEquals(42, result.longValue());
    }

    @Test
    @DisplayName("Create scalar with double, read on MASTER without keyHint")
    void testCreateReadDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "scalar_no_hint_double_" + UUID.randomUUID();
        double value = 3.14;

        KeyHintData hint = client.create(key, vd(value)).get();
        Thread.sleep(500);

        Variant result = client.setMode(Mode.MASTER).get(key).get();
        assertNotNull(result);
        assertEquals(3.14, result.doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. TYPE SWITCHING WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("Replace bytes with int on MASTER without keyHint")
    void testSwitchBytesToIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "switch_no_hint_bytes_to_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // Update to int on MASTER without keyHint
        client.setMode(Mode.MASTER);
        client.replace(key, vi(42)).get();
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        Variant v2 = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(42, v2.longValue());
    }

    @Test
    @DisplayName("Replace int with double on MASTER without keyHint")
    void testSwitchIntToDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "switch_no_hint_int_to_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        client.replace(key, vd(3.14)).get();
        Thread.sleep(500);

        Variant v2 = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(3.14, v2.doubleValue(), 0.001);
    }

    // =========================================================================
    // 3. GET AND DELETE WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteValue on MASTER without keyHint")
    void testGetAndDeleteValueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "get_delete_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // GetAndDelete on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Variant deleted = client.pop(key).get();
        assertNotNull(deleted);
        assertEquals(100, deleted.longValue());
        Thread.sleep(500);

        // Verify on BACKUP - should not exist
        Variant backupValue = client.setMode(Mode.BACKUP).get(key).get();
        assertNull(backupValue);
    }

    // =========================================================================
    // 4. TTL OPERATIONS WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("setTtl and getTtl on MASTER without keyHint")
    void testSetTtlAndGetTtlWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttl_no_hint_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(42)).get();
        Thread.sleep(500);

        // SetTtl on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Boolean setResult = client.setTtl(key, 5000).get();
        assertTrue(setResult);
        Thread.sleep(500);

        // GetTtl on BACKUP without keyHint
        Long ttl = client.setMode(Mode.BACKUP).getTtl(key).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 5000);
    }

    // =========================================================================
    // 5. EXIST + REMOVE WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("existKey and remove on MASTER without keyHint")
    void testExistAndRemoveWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "exist_remove_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("test")).get();
        Thread.sleep(500);

        // ExistKey on MASTER without keyHint
        Boolean exists = client.setMode(Mode.MASTER).containsKey(key).get();
        assertTrue(exists);

        // Remove on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Boolean removed = client.remove(key).get();
        assertTrue(removed);
        Thread.sleep(500);

        // Verify on BACKUP - should not exist
        exists = client.setMode(Mode.BACKUP).containsKey(key).get();
        assertFalse(exists);
    }

    // =========================================================================
    // 6. LOCK OPERATIONS WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on MASTER without keyHint: owner and intruder can both read")
    void testReadLockWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "read_lock_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP without keyHint
        Variant val = client.setMode(Mode.BACKUP).get(key, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        Variant intruderVal = client.setMode(Mode.BACKUP).get(key, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on MASTER without keyHint: owner can read/write")
    void testWriteLockWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "write_lock_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP without keyHint
        Variant val = client.setMode(Mode.BACKUP).get(key, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER without keyHint
        client.replace(key, v("updated"), 100).get();
        Thread.sleep(500);

        // Verify on BACKUP - should be updated
        val = client.setMode(Mode.BACKUP).get(key, 100).get();
        assertEquals("updated", new String(val.bytesValue(), StandardCharsets.UTF_8));

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on MASTER without keyHint: only owner can access")
    void testGlobalLockWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "global_lock_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP without keyHint
        Variant val = client.setMode(Mode.BACKUP).get(key, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on scalar without keyHint")
    void testLockExpirationWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "lock_expire_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP without keyHint
        Variant val = client.setMode(Mode.BACKUP).get(key, 100).get();
        assertNotNull(val);
    }
}