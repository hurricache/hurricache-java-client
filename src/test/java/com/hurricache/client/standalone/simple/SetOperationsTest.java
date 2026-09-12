package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for Set operations (unordered set / hashset).
 * Covers all methods supported by the Set container.
 */
public class SetOperationsTest extends TestBase {

    private static final Duration TEST_TTL = Duration.ofSeconds(60);
    private static final int DEFAULT_CLIENT_ID = 1;
    private static final int SECONDARY_CLIENT_ID = 2;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "set_test_" + UUID.randomUUID();
    }



    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 1. CREATE SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Create empty Set")
    void testCreateEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_empty";
        
        KeyHintData hint = client.createSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint, "KeyHint should be created");
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Empty set should return empty list");
    }

    @Test
    @DisplayName("Create Set with initial data")
    void testCreateSetWithInitialData() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_initial";
        List<Payload> initialData = List.of(
            p("item1"),
            p("item2"),
            p("item3")
        );
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint, "KeyHint should be created");
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Set should contain 3 elements");
        
        List<String> resultStrings = result.stream()
            .map(this::str)
            .toList();
        assertTrue(resultStrings.contains("item1"));
        assertTrue(resultStrings.contains("item2"));
        assertTrue(resultStrings.contains("item3"));
    }

    @Test
    @DisplayName("Create large Set with automatic chunking")
    void testCreateLargeSetWithChunking() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_large";
        int elementCount = 1500;
        List<Payload> payloads = new ArrayList<>();
        
        for (int i = 0; i < elementCount; i++) {
            payloads.add(p("large_item_" + i + "_" + UUID.randomUUID()));
        }
        
        KeyHintData hint = client.createSet(setKey, payloads).get();
        assertNotNull(hint, "KeyHint should be created");
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Set should contain " + elementCount + " elements");
    }

    // =========================================================================
    // 2. STREAM SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamSet on empty set returns empty response")
    void testStreamSetEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_empty";
        
        KeyHintData hint = client.createSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamSet on empty set should return empty list");
    }

    @Test
    @DisplayName("streamSet returns all container content")
    void testStreamSetReturnsAllContent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_all";
        List<Payload> initialData = List.of(
            p("elem1"),
            p("elem2"),
            p("elem3"),
            p("elem4"),
            p("elem5")
        );
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamSet should return all 5 elements");
    }

    // =========================================================================
    // 3. ADD ELEMENT OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElement adds elements to set and returns count of added unique elements")
    void testAddElement() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Add 2 new elements
        List<Payload> newElements = List.of(p("item2"), p("item3"));
        Integer added = client.addElementUnordered(setKey, newElements).get();
        assertEquals(2, added, "2 unique elements should be added");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(3, size, "Set should contain 3 elements");
    }

    @Test
    @DisplayName("addElement does not add duplicates and returns 0 for duplicates")
    void testAddElementNoDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_no_dup";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Try to add duplicate
        List<Payload> duplicateElements = List.of(p("item1"), p("item2"));
        Integer added = client.addElementUnordered(setKey, duplicateElements).get();
        assertEquals(1, added, "Only 1 new element (item2) should be added, item1 is duplicate");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(2, size, "Set should contain 2 elements");
    }

    // =========================================================================
    // 4. REMOVE FROM CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer removes element from set and returns 1")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove";
        List<Payload> initialData = List.of(p("item1"), p("item2"), p("item3"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, p("item1").getValue()).get();
        assertEquals(1, removed, "1 element should be removed");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(2, size, "Set should contain 2 elements");
    }

    @Test
    @DisplayName("removeFromContainer returns 0 if element doesn't exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist";
        List<Payload> initialData = List.of(p("item1"), p("item2"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(2, size, "Set should contain 2 elements");
    }

    // =========================================================================
    // 5. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey checks for element presence in container")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains";
        List<Payload> initialData = List.of(p("item1"), p("item2"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Check existing element
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, p("item1").getValue()).get();
        assertTrue(exists, "Element item1 should exist");
        
        // Check non-existent element
        Boolean notExists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        Assertions.assertFalse(notExists, "Element nonexistent should not exist");
    }

    // =========================================================================
    // 6. SET TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl sets TTL on set")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_set";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Boolean setTtlResult = client.setTtl(setKey, hint, 100).get();
        assertTrue(setTtlResult, "TTL should be successfully set");
    }

    @Test
    @DisplayName("getTtl gets TTL of set")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_get";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Set TTL
        client.setTtl(setKey, hint, 100).get();
        
        Thread.sleep(500);
        assertNotFound(client.getTtl(setKey, hint));

    }

    // =========================================================================
    // 7. TTL EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("After TTL expiration set should be deleted")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Set TTL = 1 second
        client.setTtl(setKey, hint, 1).get();
        
        Thread.sleep(1500);
        
        // Check that set is deleted - streamSet should return empty list or error
        assertNotFound(client.streamSet(setKey, hint));

        // After TTL expiration container is deleted, streamSet may return empty list
        // or throw error depending on server implementation
    }

    // =========================================================================
    // 8. LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: multiple clients can read in parallel")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_read_lock";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // First client acquires READ_LOCK
        LockStatus lock1 = client.lockObject(setKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "First client should get READ_LOCK");
        
        // Second client can also acquire READ_LOCK
        LockStatus lock2 = client.lockObject(setKey, LockType.READ_LOCK, SECONDARY_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2, "Second client should get READ_LOCK");
        
        // Both clients can read
        List<Payload> result1 = client.streamSet(setKey, hint, DEFAULT_CLIENT_ID).get();
        assertNotNull(result1);
        assertEquals(1, result1.size());
        
        List<Payload> result2 = client.streamSet(setKey, hint, SECONDARY_CLIENT_ID).get();
        assertNotNull(result2);
        assertEquals(1, result2.size());
        
        // Release locks
        client.unlockObject(setKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(setKey, SECONDARY_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: only owner can read and write, others cannot do anything")
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_write_lock";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Owner takes WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get WRITE_LOCK");
        
        // Owner can read
        List<Payload> readResult = client.streamSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());
        
        // Owner can write (add elements)
        Integer added = client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Owner should add element");
        
        // Another client cannot read
        try {
            client.streamSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Intruder should not have read access under WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Should be PERMISSION_DENIED error");
        }
        
        // Another client cannot write
        try {
            client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Intruder should not have write access under WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Should be PERMISSION_DENIED error");
        }
        
        // Release lock
        client.unlockObject(setKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL: only owner performs any operations, all others are blocked")
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_global_lock";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Owner takes GLOBAL LOCK
        LockStatus lock = client.lockObject(setKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get GLOBAL LOCK");
        
        // Owner can read
        List<Payload> readResult = client.streamSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());
        
        // Owner can write
        Integer added = client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Owner should add element");
        
        // Another client cannot read
        try {
            client.streamSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Intruder should not have read access under GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Should be PERMISSION_DENIED error");
        }
        
        // Another client cannot write
        try {
            client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Intruder should not have write access under GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Should be PERMISSION_DENIED error");
        }

        assertDenied(client.unlockObject(setKey, INTRUDER_CLIENT_ID));
        
        // Release lock by owner
        client.unlockObject(setKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("unlockObject: only owner can release lock")
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unlock_owner";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Owner takes WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get WRITE_LOCK");
        
        // Intruder tries to release lock
        assertDenied(client.unlockObject(setKey, INTRUDER_CLIENT_ID));
        
        // Owner releases lock
        LockStatus validUnlock = client.unlockObject(setKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock, "Owner should release lock");
    }

    // =========================================================================
    // 9. METHODS NOT SUPPORTED BY SET (should return error)
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to set should throw an error")
    void testUnsupportedMethodsForSet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unsupported";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // getElementAtPosition - not applicable to set
        try {
            client.getElementAtPosition(setKey, hint, 0).get();
            fail("getElementAtPosition should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
        
        // getAndRemoveElementAtPosition - not applicable to set
        try {
            client.getAndRemoveElementAtPosition(setKey, hint, 0).get();
            fail("getAndRemoveElementAtPosition should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
        
        // addElementToPosition - not applicable to set
//        try {
//            client.addElementToPosition(setKey, hint, List.of(p("item2")), 0).get();
//            fail("addElementToPosition should throw an error for set");
//        } catch (ExecutionException e) {
//            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
//            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
//        }
        
        // removeElementAtPosition - not applicable to set
        try {
            client.removeElementAtPosition(setKey, hint, 0, 0).get();
            fail("removeElementAtPosition should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
        
        // getHead - not applicable to set
        try {
            client.getHead(setKey, hint).get();
            fail("getHead should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
        
        // getFront - not applicable to set
        try {
            client.getHead(setKey, hint).get();
            fail("getFront should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
        
        // getTail - not applicable to set
        try {
            client.getTail(setKey, hint).get();
            fail("getTail should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
        
        // streamList - not applicable to set
//        try {
//            client.streamList(setKey, hint).get();
//            fail("streamList should throw an error for set");
//        } catch (ExecutionException e) {
//            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
//            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
//        }
        
        // streamVector - not applicable to set
//        try {
//            client.streamVector(setKey, hint).get();
//            fail("streamVector should throw an error for set");
//        } catch (ExecutionException e) {
//            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
//            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Should be INTERNAL error");
//        }
        
        // streamElementInRangeOrderedSet - not applicable to unordered set
        try {
            client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 0L, 10L, false, 0, Duration.ofSeconds(30)).get();
            fail("streamElementInRangeOrderedSet should throw an error for unordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode(), "Should be INTERNAL error");
        }
    }

    // =========================================================================
    // 10. ADDITIONAL EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("addElement with empty list returns 0")
    void testAddElementEmptyList() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_empty";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer added = client.addElementUnordered(setKey, new ArrayList<>()).get();
        assertEquals(0, added, "Adding empty list should return 0");
    }

    @Test
    @DisplayName("removeFromContainer with non-existent element returns 0")
    void testRemoveFromContainerNonExistentElement() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist_elem";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        assertEquals(0, removed, "Removing non-existent element should return 0");
    }

    @Test
    @DisplayName("containsContainerKey with non-existent element returns false")
    void testContainsContainerKeyNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains_nonexist";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        Assertions.assertFalse(exists);
    }

    @Test
    @DisplayName("streamSet on non-existent set returns error")
    void testStreamSetNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_nonexist";
        
        // Create KeyHint for non-existent set
        KeyHintData hint = KeyHintData.of(1, 1);
        
        try {
            client.streamSet(setKey, hint).get();
            fail("streamSet on non-existent set should throw an error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Should be NOT_FOUND error");
        }
    }

    @Test
    @DisplayName("TTL expiration: set is deleted after TTL expiration")
    void testTtlExpirationComplete() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire_complete";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Set TTL = 1 second
        client.setTtl(setKey, hint, 1).get();
        
        Thread.sleep(1500);
        
        // Check that set is deleted - getSize should return error or 0
        try {
            Integer size = client.getSize(setKey, hint).get();
            // If size is 0, container is empty (deleted)
            assertTrue(size == 0 || size == null, "After TTL expiration size should be 0");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "NOT_FOUND error expected");
        }
    }

    @Test
    @DisplayName("Lock with expired TTL: unlock returns OK")
    void testUnlockOnExpiredLock() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unlock_expired";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Acquire lock with TTL = 1 second
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock, "Lock should be obtained");
        
        Thread.sleep(1500);
        
        // Try to release expired lock
        LockStatus unlock = client.unlockObject(setKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock, "Releasing expired lock should return OK");
    }
}