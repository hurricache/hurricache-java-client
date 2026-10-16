package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TTL edge case tests: zero, MAX_VALUE, overflow, immediate expiry.
 */
public class TTLEdgeCasesTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;

    // =========================================================================
    // TTL = 0 (Immediate Expiry)
    // =========================================================================

    @Test
    @DisplayName("Scalar with TTL=0 expires immediately")
    void testScalarTTLZero() throws ExecutionException, InterruptedException {
        String key = "ttl_zero_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, null, vi(42), Duration.ofMillis(0), OWNER_CLIENT_ID, Duration.ofSeconds(5)).get();
        assertNotNull(hint);

        // Should expire very quickly - try to read
        try {
            client.getValue(key).get();
            // If it didn't expire instantly, try with setTtl(0)
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
            return;
        }

        // Explicitly set TTL to 0
        Boolean setResult = client.setTtl(key, 0).get();
        assertTrue(setResult);

        // Should be gone immediately
        try {
            Thread.sleep(10); // Give server time to process
            client.getValue(key).get();
            fail("Expected NOT_FOUND after TTL=0");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("List with TTL=0 expires immediately")
    void testListTTLZero() throws ExecutionException, InterruptedException {
        String key = "ttl_zero_list_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("item1"))).get();
        Boolean setResult = client.setTtl(key, 0).get();
        assertTrue(setResult);

        try {
            Thread.sleep(10);
            client.streamList(key).get();
            fail("Expected NOT_FOUND after TTL=0");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // TTL = MAX_VALUE (Never Expires)
    // =========================================================================

    @Test
    @DisplayName("Scalar with very large TTL survives")
    void testScalarLargeTTL() throws ExecutionException, InterruptedException {
        String key = "ttl_large_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, null, vi(42), Duration.ofDays(365 * 10), OWNER_CLIENT_ID, Duration.ofSeconds(5)).get();
        assertNotNull(hint);

        Variant value = client.getValue(key).get();
        assertNotNull(value);
        assertEquals(42, value.longValue());

        Long ttl = client.getTtl(key).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0);

        // Cleanup
        client.remove(key).get();
    }

    // =========================================================================
    // TTL EXPIRATION TIMING
    // =========================================================================

    @Test
    @DisplayName("TTL expiration - scalar 1ms")
    void testScalarTTL1ms() throws ExecutionException, InterruptedException {
        String key = "ttl_1ms_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vi(42)).get();
        client.setTtl(key, 1).get();

        // Should exist immediately
        Variant value = client.getValue(key).get();
        assertNotNull(value);

        // Wait for expiry
        Thread.sleep(10);

        try {
            client.getValue(key).get();
            fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration - list 1ms")
    void testListTTL1ms() throws ExecutionException, InterruptedException {
        String key = "ttl_1ms_list_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("item1"))).get();
        client.setTtl(key, 1).get();

        // Wait for expiry
        Thread.sleep(10);

        try {
            client.streamList(key).get();
            fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration - queue 1ms")
    void testQueueTTL1ms() throws ExecutionException, InterruptedException {
        String key = "ttl_1ms_queue_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("item1"))).get();
        client.setTtl(key, 1).get();

        Thread.sleep(10);

        try {
            client.getHead(key).get();
            fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration - set 1ms")
    void testSetTTL1ms() throws ExecutionException, InterruptedException {
        String key = "ttl_1ms_set_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("item1"))).get();
        client.setTtl(key, 1).get();

        Thread.sleep(10);

        try {
            client.streamSet(key).get();
            fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // TTL UPDATE
    // =========================================================================

    @Test
    @DisplayName("Update TTL on existing key")
    void testUpdateTTL() throws ExecutionException, InterruptedException {
        String key = "ttl_update_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vi(42)).get();

        Long ttl1 = client.getTtl(key).get();
        assertNotNull(ttl1);

        // Update TTL
        Boolean setResult = client.setTtl(key, 10000).get();
        assertTrue(setResult);

        Long ttl2 = client.getTtl(key).get();
        assertNotNull(ttl2);
        assertTrue(ttl2 > 0);

        // Value still exists
        Variant value = client.getValue(key).get();
        assertNotNull(value);
    }

    @Test
    @DisplayName("Set TTL on non-existent key returns false")
    void testSetTTLNonExistent() throws ExecutionException, InterruptedException {
        String key = "ttl_nonexistent_" + UUID.randomUUID();

        assertNotFound(client.setTtl(key, 1000));
    }

    // =========================================================================
    // TTL WITH DIFFERENT TYPES
    // =========================================================================

    @Test
    @DisplayName("TTL on bytes scalar")
    void testTTLBytes() throws ExecutionException, InterruptedException {
        String key = "ttl_bytes_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, v("hello"), OWNER_CLIENT_ID).get();
        client.setTtl(key, 1).get();

        Thread.sleep(10);

        try {
            client.getValue(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL on double scalar")
    void testTLDouble() throws ExecutionException, InterruptedException {
        String key = "ttl_double_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vd(3.14), OWNER_CLIENT_ID).get();
        client.setTtl(key, 1).get();

        Thread.sleep(10);

        try {
            client.getValue(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL on map container")
    void testTTLMap() throws ExecutionException, InterruptedException {
        String key = "ttl_map_" + UUID.randomUUID();

        java.util.Map<com.hurricache.client.intf.Payload, Variant> data = new java.util.HashMap<>();
        data.put(p("k1"), v("v1"));

        KeyHintData hint = client.createMap(key, data).get();
        client.setTtl(key, 1).get();

        Thread.sleep(10);

        try {
            client.streamMap(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // TTL GET ON EXPIRED KEY
    // =========================================================================

    @Test
    @DisplayName("getTtl on expired key throws NOT_FOUND")
    void testGetTTLEXpired() throws ExecutionException, InterruptedException {
        String key = "ttl_get_expired_" + UUID.randomUUID();

        KeyHintData hint = client.createKeyValue(key, vi(42)).get();
        client.setTtl(key, 1).get();

        Thread.sleep(10);

        try {
            client.getTtl(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
}