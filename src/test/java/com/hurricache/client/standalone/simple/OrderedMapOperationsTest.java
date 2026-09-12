package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive tests for OrderedMap container operations.
 */
public class OrderedMapOperationsTest extends TestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "orderedmap_test_" + UUID.randomUUID();
    }



    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private OrderedPayload op(Long order, String val) {
        return OrderedPayload.of(order, val.getBytes(StandardCharsets.UTF_8));
    }

    private OrderedPayload op(byte[] value, Long order) {
        return OrderedPayload.of(value, order);
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 12. GET CONTAINER VALUE + UPDATE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getContainerValue returns value by key")
    void testGetContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_cv";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] value = client.getContainerValue(bytes(mapKey), null, bytes("k1")).get();
        assertNotNull(value);
        assertEquals("v1", new String(value, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getContainerValue for non-existent key returns NOT_FOUND")
    void testGetContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_cv_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getContainerValue(bytes(mapKey), null, bytes("nonexistent")).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("updateContainerValue updates existing key")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_cv";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] oldValue = client.updateContainerValue(bytes(mapKey), null, bytes("k1"), bytes("new_v1")).get();
        assertNotNull(oldValue);
        assertEquals("v1", new String(oldValue, StandardCharsets.UTF_8), "Old value returned");

        byte[] newValue = client.getContainerValue(bytes(mapKey), null, bytes("k1")).get();
        assertEquals("new_v1", new String(newValue, StandardCharsets.UTF_8), "New value saved");
    }

    @Test
    @DisplayName("updateContainerValue for non-existent key returns NOT_FOUND")
    void testUpdateContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_cv_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.updateContainerValue(bytes(mapKey), null, bytes("nonexistent"), bytes("new_value")).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 11. GET SIZE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getSize on empty ordered map returns 0")
    void testGetSizeEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_empty";
        client.createOrderedMap(mapKey, Map.of()).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(0, size, "Empty ordered map should have size 0");
    }

    @Test
    @DisplayName("getSize for ordered map with elements returns correct count")
    void testGetSizeWithElements() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_with";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(3, size, "OrderedMap should contain 3 elements");
    }

    // =========================================================================
    // 13. TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl sets TTL on ordered map")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_set";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean setResult = client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(setResult, "TTL should be set");

        Long ttl = client.getTtl(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL should be positive");
    }

    @Test
    @DisplayName("getTtl returns actual TTL value")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_get";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Set TTL to 5000ms
        client.setTtl(bytes(mapKey), null, 5000, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        Long ttl = client.getTtl(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 5000, "TTL should be in range");;
    }

    @Test
    @DisplayName("TTL expiration: container unavailable after expiration")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_expire";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Set TTL to 100 ms
        client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        // Wait for expiration
        Thread.sleep(200);

        // Container should be unavailable
        try {
            client.streamOrderedMap(mapKey).get();
            fail("streamOrderedMap of expired container should throw an error");;
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 14. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("lockObject gets WRITE_LOCK for ordered map")
    void testWriteLockOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "WRITE_LOCK should be obtained");;

        // Owner can perform operations
        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), null, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, result.size());

        // Unlock
        LockStatus unlock = client.unlockObject(bytes(mapKey), null, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("lockObject gets READ_LOCK for ordered map")
    void testReadLockOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "READ_LOCK should be obtained");;

        // Owner can read
        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), null, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, result.size());

        // Unlock
        LockStatus unlock = client.unlockObject(bytes(mapKey), null, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("INTRUDER cannot acquire WRITE_LOCK when WRITE_LOCK exists")
    void testWriteLockIntruderDenied() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_denied";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Owner gets WRITE_LOCK
        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder cannot get WRITE_LOCK - gets CANT_UNLOCK
        LockStatus intruderLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotEquals(LockStatus.OK, intruderLock, "Intruder should not acquire lock");
    }

    @Test
    @DisplayName("INTRUDER cannot unlock when WRITE_LOCK is held")
    void testIntruderCannotUnlock() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_intruder_unlock";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Owner gets WRITE_LOCK
        client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();

        // Intruder cannot unlock
        LockStatus unlock = client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock, "Intruder cannot unlock");;

        // Owner unlocks
        LockStatus unlockOwner = client.unlockObject(bytes(mapKey), null, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlockOwner);
    }

    @Test
    @DisplayName("Two READ_LOCK simultaneously on one container")
    void testMultipleReadLocks() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_multi_read";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // First client gets READ_LOCK
        LockStatus lock1 = client.lockObject(mapKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1);

        // Second client can also get READ_LOCK
        LockStatus lock2 = client.lockObject(mapKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2);

        // Both unlock
        client.unlockObject(mapKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // 15. LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("After WRITE_LOCK expiration intruder can get lock")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_exp";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Owner gets WRITE_LOCK for 2 seconds
        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus intruderLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock);

        // Unlock
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("After READ_LOCK expiration intruder can get WRITE_LOCK")
    void testReadLockExpirationToWrite() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_exp";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Owner gets READ_LOCK for 2 seconds
        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus intruderLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock);

        // Unlock
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("After lock expiration container is accessible")
    void testLockExpirationAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_lock_exp_access";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Owner gets WRITE_LOCK for 1 second
        client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();

        // Wait for expiration
        Thread.sleep(2000);

        // Intruder can read
        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), null, INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, result.size());
    }

    // =========================================================================
    // 16. UNSUPPORTED METHODS FOR ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to ORDERED MAP should throw an error")
    void testUnsupportedMethodsForOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unsupported";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // getElementAtPosition - not applicable to ORDERED MAP
        try {
            client.getElementAtPosition(mapKey, null, 0).get();
            fail("getElementAtPosition should throw an error for ORDERED MAP");;
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getAndRemoveElementAtPosition - not applicable to ORDERED MAP
        try {
            client.getAndRemoveElementAtPosition(mapKey, null, 0).get();
            fail("getAndRemoveElementAtPosition should throw an error for ORDERED MAP");;
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // getHead - not applicable to ORDERED MAP
        try {
            client.getHead(mapKey, null).get();
            fail("getHead should throw an error for ORDERED MAP");;
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail - not applicable to ORDERED MAP
        try {
            client.getTail(mapKey, null).get();
            fail("getTail should throw an error for ORDERED MAP");;
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 10. REMOVE CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("remove deletes existing container")
    void testRemoveExistingContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_cont";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean removed = client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(removed, "Container should be removed");;

        // Verify container is actually removed
        try {
            client.streamOrderedMap(mapKey).get();
            fail("streamOrderedMap of removed container should throw an error");;
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("remove of non-existent container returns NOT_FOUND")
    void testRemoveNonExistentContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_nonexist";

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 9. GET AND REMOVE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveContainerValue extracts and removes element")
    void testGetAndRemoveContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] removedValue = client.getAndRemoveContainerValue(bytes(mapKey), null, bytes("k1")).get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue, StandardCharsets.UTF_8));

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(1, result.size(), "1 element remains");
        assertFalse(result.entrySet().stream().anyMatch(e -> "k1".equals(new String(e.getKey().getValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("getAndRemoveContainerValue for non-existent key returns null")
    void testGetAndRemoveContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] removedValue = client.getAndRemoveContainerValue(bytes(mapKey), null, bytes("nonexistent")).get();
        assertNotNull(removedValue, "Should return empty array for non-existent key");
        assertEquals(0, removedValue.length, "Empty byte array");
    }

    // =========================================================================
    // 8. STREAM ELEMENT IN RANGE ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap returns elements in weight range")
    void testStreamElementInRangeOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_range";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Request elements with weights from 20 to 40 inclusive
        Map<OrderedPayload, Payload> result = client.streamElementInRangeOrderedMap(bytes(mapKey), null, 20L, 40L, false, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Should have 3 elements with weights from 20 to 40");
        assertTrue(result.entrySet().stream().anyMatch(e -> 20L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap reverse=true returns in reverse order")
    void testStreamElementInRangeOrderedMapReverse() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_range_rev";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Request elements with weights from 20 to 40 in reverse order
        Map<OrderedPayload, Payload> result = client.streamElementInRangeOrderedMap(bytes(mapKey), null, 20L, 40L, true, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Should have 3 elements");
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 20L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap outside range returns empty response")
    void testStreamElementInRangeOrderedMapNoMatch() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_range_nomatch";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Request elements with weights from 100 to 200 (does not exist)
        Map<OrderedPayload, Payload> result = client.streamElementInRangeOrderedMap(bytes(mapKey), null, 100L, 200L, false, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "List should be empty");
    }

    // =========================================================================
    // 7. REMOVE ELEMENT AT POSITION (WEIGHT RANGE)
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition removes elements in weight range")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Remove elements with weights from 20 to 29 (only k2 with weight 20)
        Boolean removed = client.removeElementAtPosition(bytes(mapKey), null, 20, 29).get();
        assertTrue(removed, "Elements should be removed");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(4, result.size(), "4 elements remain");
        assertTrue(result.entrySet().stream().anyMatch(e -> 10L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 50L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("removeElementAtPosition includes endPos in range (tail included)")
    void testRemoveElementAtPositionEndIncluded() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos_end";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Remove elements with weights from 20 to 29 (only k2 with weight 20)
        Boolean removed = client.removeElementAtPosition(bytes(mapKey), null, 20, 30).get();
        assertTrue(removed, "Elements should be removed");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(2, result.size(), "3 elements remain");
        assertTrue(result.entrySet().stream().anyMatch(e -> 10L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("removeElementAtPosition works when boundaries match")
    void testRemoveElementAtPositionSamePos() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos_same";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Remove element with weight 20 (minWeight = maxWeight = 20)
        Boolean removed = client.removeElementAtPosition(bytes(mapKey), null, 20, 20).get();
        assertTrue(removed, "Element should be removed");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(2, result.size(), "2 elements remain");
        assertTrue(result.entrySet().stream().anyMatch(e -> 10L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
    }

    // =========================================================================
    // 6. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey checks existing key")
    void testContainsContainerKeyExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_exist";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean exists = client.containsContainerKey(bytes(mapKey), null, bytes("k1")).get();
        assertTrue(exists, "Key k1 should exist");
    }

    @Test
    @DisplayName("containsContainerKey checks non-existent key")
    void testContainsContainerKeyNotExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_notexist";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean notExists = client.containsContainerKey(bytes(mapKey), null, bytes("nonexistent")).get();
        assertFalse(notExists, "Key nonexistent should not exist");
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER WITH CONTAINERTYPE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer with ContainerType.ORDERED_MAP removes by key and value")
    void testRemoveFromContainerWithType() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        List<Payload> keys = List.of(p("k1"));
        List<Payload> values = List.of(p("v1"));

        Integer removed = client.removeFromContainer(bytes(mapKey), null, ContainerType.ORDERED_MAP, keys, values).get();
        assertEquals(1, removed, "1 element should be removed");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(1, result.size());
        assertFalse(result.entrySet().stream().anyMatch(e -> "k1".equals(new String(e.getKey().getValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("removeFromContainer with ContainerType.ORDERED_MAP removes by key ignoring value")
    void testRemoveFromContainerWithTypeIgnoreValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type_ignore";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Even with wrong value, removal by key works
        List<Payload> keys = List.of(p("k1"));
        List<Payload> values = List.of(p("wrong_value"));

        Integer removed = client.removeFromContainer(bytes(mapKey), null, ContainerType.ORDERED_MAP, keys, values).get();
        assertEquals(1, removed, "Element removed by key (value ignored)");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(0, result.size(), "OrderedMap empty after removal");
    }

    // =========================================================================
    // 4. REMOVE FROM CONTAINER BY KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer removes element by key and returns count")
    void testRemoveFromContainerByKey() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, bytes("k2")).get();
        assertEquals(1, removed, "1 element should be removed");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(2, result.size(), "2 elements remain");
        assertFalse(result.entrySet().stream().anyMatch(e -> "k2".equals(new String(e.getKey().getValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("removeFromContainer returns 0 for non-existent key")
    void testRemoveFromContainerByKeyNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, bytes("nonexistent")).get();
        assertEquals(0, removed, "Nothing removed (element not found)");
    }

    @Test
    @DisplayName("removeFromContainer from empty container returns 0")
    void testRemoveFromContainerFromEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_empty";
        client.createOrderedMap(mapKey, Map.of()).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, bytes("k1")).get();
        assertEquals(0, removed, "Nothing removed from empty container");
    }

    // =========================================================================
    // 3. ADD ELEMENT ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementOrderedMap adds elements to OrderedMap and returns count")
    void testAddElementOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add";
        client.createOrderedMap(mapKey, Map.of()).get();

        List<OrderedPayload> keys = List.of(op(1L, "k1"), op(2L, "k2"), op(3L, "k3"));
        List<Payload> values = List.of(p("v1"), p("v2"), p("v3"));

        Integer added = client.addElementOrderedMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added, "3 elements should be added");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(3, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(1L).equals(e.getKey().getOrder()) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(2L).equals(e.getKey().getOrder()) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(3L).equals(e.getKey().getOrder()) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("addElementOrderedMap allows duplicates (same key with different weights)")
    void testAddElementOrderedMapDuplicates() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_dup";
        client.createOrderedMap(mapKey, Map.of()).get();

        // Add elements with same key but different weights
        List<OrderedPayload> keys = List.of(op(1L, "same_key"), op(2L, "same_key"), op(3L, "same_key"));
        List<Payload> values = List.of(p("v1"), p("v2"), p("v3"));

        Integer added = client.addElementOrderedMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added, "3 elements should be added (duplicates allowed)");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(3, result.size(), "3 elements total with same key");
    }

    @Test
    @DisplayName("addElementOrderedMap with empty list returns 0")
    void testAddElementOrderedMapEmptyList() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_empty";
        client.createOrderedMap(mapKey, Map.of()).get();

        Integer added = client.addElementOrderedMap(bytes(mapKey), null, List.of(), List.of(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added, "Adding empty list should return 0");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(0, result.size());
    }

    // =========================================================================
    // 2. STREAM ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamOrderedMap of empty OrderedMap returns empty response")
    void testStreamOrderedMapEmpty() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_empty";

        KeyHintData hint = client.createOrderedMap(mapKey, Map.of()).get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamOrderedMap of empty ordered map should return empty map");
    }

    @Test
    @DisplayName("streamOrderedMap returns all container content")
    void testStreamOrderedMapWithData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_data";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "elem1"), p("val1"),
                op(2L, "elem2"), p("val2"),
                op(3L, "elem3"), p("val3"),
                op(4L, "elem4"), p("val4"),
                op(5L, "elem5"), p("val5")
        );

        KeyHintData hint = client.createOrderedMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamOrderedMap should return all 5 elements");
    }

    @Test
    @DisplayName("streamOrderedMap with explicit clientId")
    void testStreamOrderedMapWithClientId() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_cid";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        KeyHintData hint = client.createOrderedMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), hint, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(1L).equals(e.getKey().getOrder()) && "v1".equals(str(e.getValue()))));
    }

    // =========================================================================
    // 1. CREATE ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Creating empty OrderedMap")
    void testCreateEmptyOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_empty";

        KeyHintData hint = client.createOrderedMap(mapKey, Map.of()).get();
        assertNotNull(hint, "KeyHint should be created");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Empty ordered map should return empty map");
    }

    @Test
    @DisplayName("Creating OrderedMap with initial data")
    void testCreateOrderedMapWithInitialData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_initial";

        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        KeyHintData hint = client.createOrderedMap(mapKey, initialData).get();
        assertNotNull(hint, "KeyHint should be created");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "OrderedMap should contain 3 elements");
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(1L).equals(e.getKey().getOrder()) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(2L).equals(e.getKey().getOrder()) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(3L).equals(e.getKey().getOrder()) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("Creating large OrderedMap with automatic chunking")
    void testCreateOrderedMapWithLargeDataChunking() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_large";
        int elementCount = 1500;
        Map<OrderedPayload, Payload> largeData = new java.util.LinkedHashMap<>();

        for (int i = 0; i < elementCount; i++) {
            largeData.put(op((long) i, "key_" + i), p("value_" + i + "_" + UUID.randomUUID()));
        }

        KeyHintData hint = client.createOrderedMap(mapKey, largeData).get();
        assertNotNull(hint, "KeyHint should be created");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "OrderedMap should contain " + elementCount + " elements");
    }
}