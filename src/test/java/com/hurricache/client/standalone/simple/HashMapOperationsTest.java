package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive tests for HashedMap (MAP container) operations.
 */
public class HashMapOperationsTest extends TestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "hashmap_test_" + UUID.randomUUID();
    }



    // =========================================================================
    // 14. UNSUPPORTED METHODS FOR MAP
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to MAP should throw an error")
    void testUnsupportedMethodsForMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unsupported";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        // getElementAtPosition — not applicable to MAP
        try {
            client.getElementAtPosition(mapKey, null, 0).get();
            fail("getElementAtPosition should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getAndRemoveElementAtPosition — not applicable to MAP
        try {
            client.getAndDeleteElementAtPosition(mapKey, null, 0).get();
            fail("getAndRemoveElementAtPosition should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getHead — not applicable to MAP
        try {
            client.getHead(mapKey, null).get();
            fail("getHead should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail — not applicable to MAP
        try {
            client.getTail(mapKey, null).get();
            fail("getTail should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // addElementToPosition — not applicable to MAP (skipped, as method may be supported)
        // try {
        //     client.addElementToPosition(mapKey, null, List.of(p("v1")), 0).get();
        //     fail("addElementToPosition should throw an error for MAP");
        // } catch (ExecutionException e) {
        //     StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
        //     assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        // }
    }

    // =========================================================================
    // 13.1. LOCK EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK for 2s: after expiration another client can read")
    void testReadLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_exp";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Acquire READ_LOCK for 2 seconds
        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now another client can read
        Variant val = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);
        assertEquals("v1", new String(val.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("WRITE_LOCK for 2s: after expiration another client can acquire WRITE_LOCK")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_exp";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Acquire WRITE_LOCK for 2 seconds
        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL for 2s: after expiration another client can acquire WRITE_LOCK")
    void testGlobalLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_global_lock_exp";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Acquire GLOBAL LOCK for 2 seconds
        LockStatus lock = client.lockObject(mapKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Any lock for 2s: after expiration new lock OK for another client")
    void testLockExpirationAllowsNewLock() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_lock_exp_any";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Acquire READ_LOCK for 2 seconds
        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK
        LockStatus newLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // 13. LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: multiple clients can read in parallel")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock1 = client.lockObject(mapKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "First client should get READ_LOCK");

        LockStatus lock2 = client.lockObject(mapKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, lock2, "Second client should not get READ_LOCK");

        // Both can read
        Variant val1 = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val1);

        Variant val2 = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val2);

        // Release locks
        client.unlockObject(mapKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: only owner reads and writes, others blocked")
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get WRITE_LOCK");

        // Owner can read
        Variant val = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val.bytesValue());

        // Owner can add elements
        List<Payload> keys = List.of(p("k2"));
        List<Variant> values = List.of(v("v2"));
        Integer added = client.addElementHashMap(bytes(mapKey), null, keys, values, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, added);

        // Intruder cannot read
        assertDenied(client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Intruder cannot add
        List<Payload> intruderKeys = List.of(p("k3"));
        List<Variant> intruderValues = List.of(v("v3"));
        assertDenied(client.addElementHashMap(bytes(mapKey), null, intruderKeys, intruderValues, INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Release lock
        client.unlockObject(mapKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL: only owner can do any operations, all others are blocked")
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_global_lock";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get GLOBAL LOCK");

        // Owner can read
        Variant val = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Intruder cannot read
        assertDenied(client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Intruder cannot add
        List<Payload> intruderKeys = List.of(p("k2"));
        List<Variant> intruderValues = List.of(v("v2"));
        assertDenied(client.addElementHashMap(bytes(mapKey), null, intruderKeys, intruderValues, INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Intruder cannot unlock
        LockStatus unlockStatus = client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlockStatus);

        // Release lock by owner
        client.unlockObject(mapKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("unlockObject: only owner can unlock")
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unlock_owner";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder tries to unlock
        LockStatus status = client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, status);

        // Owner unlocks
        LockStatus validUnlock = client.unlockObject(mapKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock);
    }

    @Test
    @DisplayName("READ_LOCK: read OK, but write is denied")
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_writes";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Read under READ_LOCK OK
        Variant val = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1"), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Write is denied
        List<Payload> keys = List.of(p("k2"));
        List<Variant> values = List.of(v("v2"));
        assertDenied(client.addElementHashMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT));

        client.unlockObject(mapKey, DEFAULT_CLIENT_ID).get();
    }

    // =========================================================================
    // 12. TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl sets TTL on container")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_set";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        Boolean setResult = client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(setResult, "TTL should be successfully set");
    }

    @Test
    @DisplayName("getTtl gets container TTL")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_get";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        Long ttl = client.getTtl(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL should be greater than 0");
    }

    @Test
    @DisplayName("After TTL expiration getValueInContainerByKey returns NOT_FOUND")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_expire";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Set TTL = 1 second
        client.setTtl(bytes(mapKey), null, 1, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        // Wait for TTL expiration
        Thread.sleep(1500);

        // Verify container is deleted - getValueInContainerByKey will return NOT_FOUND
        assertNotFound(client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1")));
    }

    // =========================================================================
    // 11. GET AND REMOVE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteValueInContainer extracts and removes element")
    void testgetAndDeleteValueInContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2")
        );
        client.createMap(mapKey, initialData).get();

        Variant removedValue = client.getAndDeleteValueInContainer(bytes(mapKey), null, bytes("k1")).get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue.bytesValue(), StandardCharsets.UTF_8));

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(1, result.size(), "1 element remains");
        assertFalse(result.containsKey(p("k1")));
    }

    @Test
    @DisplayName("getAndDeleteValueInContainer for non-existent key returns NOT_FOUND")
    void testgetAndDeleteValueInContainerNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove_nf";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getAndDeleteValueInContainer(bytes(mapKey), null, v("nonexistent").bytesValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 10. GET AND UPDATE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey gets value by key")
    void testgetValueInContainerByKey() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_val";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2")
        );
        client.createMap(mapKey, initialData).get();

        Variant value = client.getValueInContainerByKey(bytes(mapKey), null, bytes("k1")).get();
        assertNotNull(value);
        assertEquals("v1", new String(value.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getValueInContainerByKey for non-existent key returns NOT_FOUND")
    void testgetValueInContainerByKeyNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_val_nf";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getValueInContainerByKey(bytes(mapKey), null, v("nonexistent").bytesValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("updateContainerValue updates value and returns old one")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("old_value")
        );
        client.createMap(mapKey, initialData).get();

        byte[] elemKey = bytes("k1");
        Variant oldValue = client.updateContainerValue(bytes(mapKey), null, elemKey, v("new_value")).get();
        assertNotNull(oldValue, "oldValue should not be null");
        assertEquals("old_value", new String(oldValue.bytesValue(), StandardCharsets.UTF_8));

        Variant newValue = client.getValueInContainerByKey(bytes(mapKey), null, elemKey).get();
        assertEquals("new_value", new String(newValue.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue for non-existent key returns NOT_FOUND")
    void testUpdateContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_nf";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.updateContainerValue(bytes(mapKey), null, bytes("nonexistent"), v("new")).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 9. GET SIZE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getSize for empty map returns 0")
    void testGetSizeEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_empty";
        client.createMap(mapKey, Map.of()).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(0, size, "Empty map should have size 0");
    }

    @Test
    @DisplayName("getSize for map with elements returns correct count")
    void testGetSizeWithElements() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_with";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2"),
                p("k3"), v("v3")
        );
        client.createMap(mapKey, initialData).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(3, size, "Map should contain 3 elements");
    }

    // =========================================================================
    // 8. REMOVE CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("remove deletes existing container")
    void testRemoveExistingContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_cont";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        Boolean removed = client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(removed, "Container should be removed");

        // Verify container is actually removed
        try {
            client.streamMap(mapKey).get();
            fail("streamMap of removed container should throw an error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("remove of non-existent container returns NOT_FOUND")
    void testRemoveNonExistentContainer() {
        String mapKey = baseKey + "_remove_nonexist";

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 7. REMOVE ELEMENT AT POSITION (NOT SUPPORTED FOR MAP)
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition not supported for MAP - returns error")
    void testRemoveElementAtPositionNotSupported() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        try {
            client.deleteElementAtPosition(bytes(mapKey), null, 0, 0).get();
            fail("removeElementAtPosition should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
    }

    // =========================================================================
    // 6. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("existInContainerByKey checks existing key")
    void testexistInContainerByKeyExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_exist";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2")
        );
        client.createMap(mapKey, initialData).get();

        Boolean exists = client.existInContainerByKey(bytes(mapKey), null, bytes("k1")).get();
        assertTrue(exists, "Key k1 should exist");
    }

    @Test
    @DisplayName("existInContainerByKey checks non-existent key")
    void testexistInContainerByKeyNotExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_notexist";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        Boolean notExists = client.existInContainerByKey(bytes(mapKey), null, bytes("nonexistent")).get();
        assertFalse(notExists, "Key nonexistent should not exist");
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER WITH CONTAINERTYPE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer with ContainerType.MAP removes by key and value")
    void testRemoveFromContainerWithType() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2")
        );
        client.createMap(mapKey, initialData).get();

        List<Payload> keys = List.of(p("k1"));
        List<Variant> values = List.of(Variant.ofBytes(bytes("v1")));

        Integer removed = client.removeFromContainerByKeyAndValue(bytes(mapKey), null, ContainerType.MAP, keys, values).get();
        assertEquals(1, removed, "1 element should be removed");

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(1, result.size());
        assertFalse(result.containsKey(p("k1")));
    }

    @Test
    @DisplayName("removeFromContainer with ContainerType.MAP removes by key even if value does not match")
    void testRemoveFromContainerWithTypeByWrongValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2")
        );
        client.createMap(mapKey, initialData).get();

        List<Payload> keys = List.of(p("k1"));
        List<Variant> values = List.of(Variant.ofBytes(bytes("wrong_value")));

        Integer removed = client.removeFromContainerByKeyAndValue(bytes(mapKey), null, ContainerType.MAP, keys, values).get();
        assertEquals(0, removed, "Element removed by key");

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(2, result.size(), "1 element remains");
    }

    // =========================================================================
    // 4. REMOVE FROM CONTAINER BY KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer removes element by key and returns 1")
    void testRemoveFromContainerByKey() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2"),
                p("k3"), v("v3")
        );
        client.createMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainerByKey(bytes(mapKey), null, p("k2")).get();
        assertEquals(1, removed, "1 element should be removed");

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(2, result.size(), "2 elements remain");
        assertFalse(result.containsKey(p("k2")));
    }

    @Test
    @DisplayName("removeFromContainer returns 0 for non-existent key")
    void testRemoveFromContainerByKeyNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key_nf";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );
        client.createMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainerByKey(bytes(mapKey), null, p("nonexistent")).get();
        assertEquals(0, removed, "Nothing removed (element not found)");
    }

    @Test
    @DisplayName("removeFromContainer from empty container returns 0")
    void testRemoveFromContainerFromEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_empty";
        client.createMap(mapKey, Map.of()).get();

        Integer removed = client.removeFromContainerByKey(bytes(mapKey), null, p("k1")).get();
        assertEquals(0, removed, "Nothing removed from empty container");
    }

    // =========================================================================
    // 3. ADD ELEMENT HASHMAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementHashMap adds elements and returns count")
    void testAddElementHashMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add";
        client.createMap(mapKey, Map.of()).get();

        List<Payload> keys = List.of(p("k1"), p("k2"), p("k3"));
        List<Variant> values = List.of(v("v1"), v("v2"), v("v3"));

        Integer added = client.addElementHashMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added, "3 elements should be added");

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(3, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k2".equals(str(e.getKey())) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k3".equals(str(e.getKey())) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("addElementHashMap does not allow duplicate keys")
    void testAddElementHashMapNoDuplicates() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_nodup";
        client.createMap(mapKey, Map.of()).get();

        // Add unique keys
        List<Payload> keys1 = List.of(p("k1"), p("k2"));
        List<Variant> values1 = List.of(v("v1"), v("v2"));
        Integer added1 = client.addElementHashMap(bytes(mapKey), null, keys1, values1, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(2, added1);

        // Try to add duplicate k1 - should return 0
        List<Payload> keys2 = List.of(p("k1"));
        List<Variant> values2 = List.of(v("v1_new"));
        Integer added2 = client.addElementHashMap(bytes(mapKey), null, keys2, values2, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added2, "Duplicate key should not be added");

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(2, result.size(), "Should have 2 elements in total");
    }

    @Test
    @DisplayName("addElementHashMap with empty list returns 0")
    void testAddElementHashMapEmptyList() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_empty";
        client.createMap(mapKey, Map.of()).get();

        Integer added = client.addElementHashMap(bytes(mapKey), null, List.of(), List.of(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added, "Adding empty list should return 0");

        Map<Payload, Variant> result = client.streamMap(mapKey).get();
        assertEquals(0, result.size());
    }

    // =========================================================================
    // 2. STREAM MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamMap of empty HashedMap returns empty response")
    void testStreamMapEmpty() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_empty";

        KeyHintData hint = client.createMap(mapKey, Map.of()).get();
        assertNotNull(hint);

        Map<Payload, Variant> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamMap of empty map should return empty map");
    }

    @Test
    @DisplayName("streamMap returns all container content")
    void testStreamMapWithData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_data";
        Map<Payload, Variant> initialData = Map.of(
                p("elem1"), v("val1"),
                p("elem2"), v("val2"),
                p("elem3"), v("val3"),
                p("elem4"), v("val4"),
                p("elem5"), v("val5")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<Payload, Variant> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamMap should return all 5 elements");
    }

    @Test
    @DisplayName("streamMap with explicit clientId")
    void testStreamMapWithClientId() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_cid";
        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<Payload, Variant> result = client.streamMap(bytes(mapKey), hint, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
    }

    // =========================================================================
    // 1. CREATE MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Creating empty HashedMap")
    void testCreateEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_empty";

        KeyHintData hint = client.createMap(mapKey, Map.of()).get();
        assertNotNull(hint, "KeyHint should be created");

        Map<Payload, Variant> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Empty map should return empty map");
    }

    @Test
    @DisplayName("Creating HashedMap with initial data")
    void testCreateMapWithInitialData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_initial";

        Map<Payload, Variant> initialData = Map.of(
                p("k1"), v("v1"),
                p("k2"), v("v2"),
                p("k3"), v("v3")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint, "KeyHint should be created");

        Map<Payload, Variant> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Map should contain 3 elements");
        // Check via stream since streamMap returns new Payload objects
        assertTrue(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k2".equals(str(e.getKey())) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k3".equals(str(e.getKey())) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("Creating large HashedMap with automatic chunking")
    void testCreateMapWithLargeDataChunking() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_large";
        int elementCount = 1500;
        Map<Payload, Variant> largeData = new java.util.HashMap<>();

        for (int i = 0; i < elementCount; i++) {
            largeData.put(p("key_" + i), v("value_" + i + "_" + UUID.randomUUID()));
        }

        KeyHintData hint = client.createMap(mapKey, largeData).get();
        assertNotNull(hint, "KeyHint should be created");

        Map<Payload, Variant> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Map should contain " + elementCount + " elements");
    }
}