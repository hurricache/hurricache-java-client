package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ScalarOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + READ SCALAR
    // =========================================================================

    @Test
    @DisplayName("Create scalar with bytes, read back with bytesValue()")
    void testCreateReadBytes() throws ExecutionException, InterruptedException {
        String key = "scalar_bytes_" + UUID.randomUUID();
        String value = "hello_world";

        KeyHintData hint = client.createKeyValue(key, v(value)).get();
        assertNotNull(hint);

        Variant result = client.getValue(key).get();
        assertNotNull(result);
        assertEquals("hello_world", new String(result.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create scalar with int, read back with longValue()")
    void testCreateReadInt() throws ExecutionException, InterruptedException {
        String key = "scalar_int_" + UUID.randomUUID();
        int value = 42;

        KeyHintData hint = client.createKeyValue(key, vi(value)).get();
        assertNotNull(hint);

        Variant result = client.getValue(key).get();
        assertNotNull(result);
        assertEquals(42, result.longValue());
    }

    @Test
    @DisplayName("Create scalar with double, read back with doubleValue()")
    void testCreateReadDouble() throws ExecutionException, InterruptedException {
        String key = "scalar_double_" + UUID.randomUUID();
        double value = 3.14;

        KeyHintData hint = client.createKeyValue(key, vd(value)).get();
        assertNotNull(hint);

        Variant result = client.getValue(key).get();
        assertNotNull(result);
        assertEquals(3.14, result.doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. TYPE SWITCHING
    // =========================================================================

    @Test
    @DisplayName("Replace bytes with int")
    void testSwitchBytesToInt() throws ExecutionException, InterruptedException {
        String key = "switch_bytes_to_int_" + UUID.randomUUID();

        // Create with bytes
        KeyHintData hint = client.createKeyValue(key, v("hello")).get();
        Variant v1 = client.getValue(key).get();
        assertEquals("hello", new String(v1.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // Update to int
        client.updateKeyValue(key, vi(42)).get();
        Variant v2 = client.getValue(key).get();
        assertEquals(42, v2.longValue());
    }

    @Test
    @DisplayName("Replace int with double")
    void testSwitchIntToDouble() throws ExecutionException, InterruptedException {
        String key = "switch_int_to_double_" + UUID.randomUUID();

        // Create with int
        KeyHintData hint = client.createKeyValue(key, vi(100)).get();
        Variant v1 = client.getValue(key).get();
        assertEquals(100, v1.longValue());

        // Update to double
        client.updateKeyValue(key, vd(3.14)).get();
        Variant v2 = client.getValue(key).get();
        assertEquals(3.14, v2.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Replace double with bytes")
    void testSwitchDoubleToBytes() throws ExecutionException, InterruptedException {
        String key = "switch_double_to_bytes_" + UUID.randomUUID();

        // Create with double
        KeyHintData hint = client.createKeyValue(key, vd(2.5)).get();
        Variant v1 = client.getValue(key).get();
        assertEquals(2.5, v1.doubleValue(), 0.001);

        // Update to bytes
        client.updateKeyValue(key, v("new_value")).get();
        Variant v2 = client.getValue(key).get();
        assertEquals("new_value", new String(v2.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. GET AND DELETE
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteValue returns Variant and removes key")
    void testGetAndDeleteValue() throws ExecutionException, InterruptedException {
        String key = "get_delete_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vi(100)).get();

        Variant deleted = client.getAndDeleteValue(key).get();
        assertNotNull(deleted);
        assertEquals(100, deleted.longValue());

        // Key should be gone
        try {
            client.getValue(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 4. TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl and getTtl on scalar")
    void testSetTtlAndGetTtl() throws ExecutionException, InterruptedException {
        String key = "ttl_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vi(42)).get();

        Boolean setResult = client.setTtl(key, 5000).get();
        assertTrue(setResult);

        Long ttl = client.getTtl(key).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 5000);
    }

    @Test
    @DisplayName("TTL expiration on scalar")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String key = "ttl_expire_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vi(42)).get();

        client.setTtl(key, 1).get();

        Thread.sleep(2000);

        try {
            client.getValue(key).get();
            fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 5. EXIST + REMOVE
    // =========================================================================

    @Test
    @DisplayName("existKey and remove on scalar")
    void testExistAndRemove() throws ExecutionException, InterruptedException {
        String key = "exist_remove_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("test")).get();

        Boolean exists = client.existKey(key).get();
        assertTrue(exists);

        Boolean removed = client.remove(key).get();
        assertTrue(removed);

        exists = client.existKey(key).get();
        assertFalse(exists);
    }

    // =========================================================================
    // 6. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("value")).get();

        LockStatus lock = client.lockObject(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.getValue(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Variant intruderVal = client.getValue(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.updateKeyValue(key, v("updated"), OWNER_CLIENT_ID));
        assertDenied(client.updateKeyValue(key, v("updated"), INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "write_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("value")).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.getValue(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Owner can write
        client.updateKeyValue(key, v("updated"), OWNER_CLIENT_ID).get();
        val = client.getValue(key, OWNER_CLIENT_ID).get();
        assertEquals("updated", new String(val.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // Intruder denied
        assertDenied(client.getValue(key, INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "global_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("value")).get();

        LockStatus lock = client.lockObject(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.getValue(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder denied
        assertDenied(client.getValue(key, INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Unlock by owner only")
    void testUnlockOwnerOnly() throws ExecutionException, InterruptedException {
        String key = "unlock_owner_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("value")).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder cannot unlock
        LockStatus unlock = client.unlockObject(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        // Owner unlocks
        unlock = client.unlockObject(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Lock expiration on scalar")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "lock_expire_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("value")).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        // Lock expired, intruder can access
        Variant val = client.getValue(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}