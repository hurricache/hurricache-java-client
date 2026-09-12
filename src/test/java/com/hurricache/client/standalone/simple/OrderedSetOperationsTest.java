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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for Ordered Set operations (sorted set with weights).
 * Covers all methods supported by the Ordered Set container type.
 */
public class OrderedSetOperationsTest extends TestBase {

    private static final Duration TEST_TTL = Duration.ofSeconds(60);
    private static final int DEFAULT_CLIENT_ID = 1;
    private static final int SECONDARY_CLIENT_ID = 2;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "orderedset_test_" + UUID.randomUUID();
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
    // 1. CREATE ORDERED SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Create empty Ordered Set")
    void testCreateEmptyOrderedSet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_empty";

        KeyHintData hint = client.createOrderedSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint, "KeyHint must be created");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "An empty ordered set must return an empty list");
    }

    @Test
    @DisplayName("Create Ordered Set with initial data")
    void testCreateOrderedSetWithInitialData() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_initial";
        List<OrderedPayload> initialData = List.of(
                op(1L, "item1"),
                op(2L, "item2"),
                op(3L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint, "KeyHint must be created");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Ordered set must contain 3 elements");

        List<String> resultStrings = result.stream()
                .map(this::str)
                .toList();
        assertTrue(resultStrings.contains("item1"));
        assertTrue(resultStrings.contains("item2"));
        assertTrue(resultStrings.contains("item3"));

        // Verify ordering (ascending by weight)
        assertEquals(1L, result.get(0).getOrder());
        assertEquals(2L, result.get(1).getOrder());
        assertEquals(3L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("Create large Ordered Set with automatic chunking")
    void testCreateLargeOrderedSetWithChunking() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_large";
        int elementCount = 1500;
        List<OrderedPayload> payloads = new ArrayList<>();

        for (int i = 0; i < elementCount; i++) {
            payloads.add(op((long) i, "large_item_" + i + "_" + UUID.randomUUID()));
        }

        KeyHintData hint = client.createOrderedSet(setKey, payloads).get();
        assertNotNull(hint, "KeyHint must be created");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Ordered set must contain " + elementCount + " elements");
    }

    // =========================================================================
    // 2. STREAM ORDERED SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamOrderedSet on an empty set returns empty response")
    void testStreamOrderedSetEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_empty";

        KeyHintData hint = client.createOrderedSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint);

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamOrderedSet on an empty ordered set must return an empty list");
    }

    @Test
    @DisplayName("streamOrderedSet returns all container contents")
    void testStreamOrderedSetReturnsAllContent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_all";
        List<OrderedPayload> initialData = List.of(
                op(1L, "elem1"),
                op(2L, "elem2"),
                op(3L, "elem3"),
                op(4L, "elem4"),
                op(5L, "elem5")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamOrderedSet must return all 5 elements");
    }

    // =========================================================================
    // 3. ADD ELEMENT WITH WEIGHT OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementWithWeight adds elements with weights and returns added count")
    void testAddElementWithWeight() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Add 2 new elements with weights
        List<OrderedPayload> newElements = List.of(op(2L, "item2"), op(3L, "item3"));
        Integer added = client.addElementWithWeight(setKey, hint, newElements).get();
        assertEquals(2, added, "2 elements should be added");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set must contain 3 elements");

        // Verify order
        assertEquals(1L, result.get(0).getOrder());
        assertEquals(2L, result.get(1).getOrder());
        assertEquals(3L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("addElementWithWeight allows duplicates (same keys with different weights)")
    void testAddElementWithWeightAllowsDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight_dup";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Add elements with the same key but different weights (duplicates allowed)
        List<OrderedPayload> duplicateElements = List.of(op(2L, "item1"), op(3L, "item1"));
        Integer added = client.addElementWithWeight(setKey, hint, duplicateElements).get();
        assertEquals(2, added, "2 elements should be added (duplicates allowed)");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set must contain 3 elements");
    }

    @Test
    @DisplayName("addElementWithWeight with empty list returns 0")
    void testAddElementWithWeightEmptyList() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight_empty";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        Integer added = client.addElementWithWeight(setKey, hint, new ArrayList<>()).get();
        assertEquals(0, added, "Adding an empty list must return 0");
    }

    // =========================================================================
    // 4. STREAM ELEMENT IN RANGE ORDERED OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrdered returns elements within startWeight and endWeight inclusively")
    void testStreamElementInRangeOrdered() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3"),
                op(40L, "item4"),
                op(50L, "item5")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Request elements with weights from 20 to 40 inclusively
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 20L, 40L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "There should be 3 elements with weights from 20 to 40");

        // Verify correct elements
        assertEquals(20L, result.get(0).getOrder());
        assertEquals(30L, result.get(1).getOrder());
        assertEquals(40L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrdered with reverse=true returns elements in reverse order")
    void testStreamElementInRangeOrderedReverse() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_reverse";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3"),
                op(40L, "item4"),
                op(50L, "item5")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Request elements with weights from 20 to 40 in reverse order
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 20L, 40L, true, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "There should be 3 elements");

        // Verify reverse order
        assertEquals(40L, result.get(0).getOrder());
        assertEquals(30L, result.get(1).getOrder());
        assertEquals(20L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrdered returns empty list if no elements fall in range")
    void testStreamElementInRangeOrderedNoMatch() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_nomatch";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Request elements with weights from 100 to 200 (none exist)
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 100L, 200L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "List must be empty");
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer removes element by value and returns removed count")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"), op(2L, "item2"), op(3L, "item3"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(1L, "item1").getValue()).get();
        assertEquals(1, removed, "1 element should be removed");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set must contain 2 elements");
    }

    @Test
    @DisplayName("removeFromContainer can remove multiple duplicates with same key but different weights")
    void testRemoveFromContainerMultipleDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_multi";
        List<OrderedPayload> initialData = List.of(
                op(1L, "item1"),
                op(2L, "item1"),
                op(3L, "item1"),
                op(4L, "item2")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Remove all elements matching key "item1" (3 items)
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(1L, "item1").getValue()).get();
        assertEquals(3, removed, "3 elements with the same key must be removed");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(1, result.size(), "Ordered set must contain 1 element");
        assertEquals("item2", str(result.get(0)));
    }

    @Test
    @DisplayName("removeFromContainer returns 0 if element does not exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"), op(2L, "item2"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(0L, "nonexistent").getValue()).get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");
    }

    // =========================================================================
    // 6. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey checks for element presence in container")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"), op(2L, "item2"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Check existing element
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, op(1L, "item1").getValue()).get();
        assertTrue(exists, "Element item1 must exist");

        // Check non-existing element
        Boolean notExists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, op(0L, "nonexistent").getValue()).get();
        Assertions.assertFalse(notExists, "Element nonexistent must not exist");
    }

    // =========================================================================
    // 7. REMOVE ELEMENT AT WEIGHT RANGE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition removes elements within specified weight range")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_pos";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3"),
                op(40L, "item4"),
                op(50L, "item5")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Remove elements with weights from 20 to 40 inclusively
        Boolean removed = client.removeElementAtPosition(setKey, hint, 20, 40).get();
        assertTrue(removed, "Elements must be removed");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set must contain 2 elements");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(50L, result.get(1).getOrder());
    }

    @Test
    @DisplayName("removeElementAtPosition works when weight boundaries match")
    void testRemoveElementAtPositionSamePos() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_pos_same";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Remove element with weight 20 (minWeight = maxWeight = 20)
        Boolean removed = client.removeElementAtPosition(setKey, hint, 20, 20).get();
        assertTrue(removed, "Element must be removed");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set must contain 2 elements");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(30L, result.get(1).getOrder());
    }

    // =========================================================================
    // 8. SET TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl sets a TTL on the ordered set")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_set";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        Boolean setTtlResult = client.setTtl(setKey, hint, 100).get();
        assertTrue(setTtlResult, "TTL must be successfully set");
    }

    @Test
    @DisplayName("getTtl retrieves TTL of the ordered set")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_get";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Set TTL
        client.setTtl(setKey, hint, 100).get();
        Thread.sleep(150);

        assertNotFound(client.getTtl(setKey, hint));
    }

    // =========================================================================
    // 9. TTL EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Ordered set is deleted after TTL expiration")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Set short TTL = 1 second
        client.setTtl(setKey, hint, 1).get();

        Thread.sleep(1500);

        // Verify set is deleted - streamOrderedSet should return empty or error
        assertNotFound(client.streamOrderedSet(setKey, hint));
    }

    // =========================================================================
    // 10. LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: multiple clients can read concurrently")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_read_lock";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // First client acquires READ_LOCK
        LockStatus lock1 = client.lockObject(setKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "First client should acquire READ_LOCK");

        // Second client also acquires READ_LOCK
        LockStatus lock2 = client.lockObject(setKey, LockType.READ_LOCK, SECONDARY_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2, "Second client should acquire READ_LOCK");

        // Both clients can read
        List<OrderedPayload> result1 = client.streamOrderedSet(setKey, hint, DEFAULT_CLIENT_ID).get();
        assertNotNull(result1);
        assertEquals(1, result1.size());

        List<OrderedPayload> result2 = client.streamOrderedSet(setKey, hint, SECONDARY_CLIENT_ID).get();
        assertNotNull(result2);
        assertEquals(1, result2.size());

        // Release locks
        client.unlockObject(setKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(setKey, SECONDARY_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: only the owner can read and write, others are blocked")
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_write_lock";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Owner acquires WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should acquire WRITE_LOCK");

        // Owner can read
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());

        // Owner can write (add elements)
        Integer added = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(2L, "item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Owner should add element");

        // Intruder cannot read
        try {
            client.streamOrderedSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Intruder must not have read access under WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Must throw PERMISSION_DENIED");
        }

        // Intruder cannot write
        try {
            client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(3L, "item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Intruder must not have write access under WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Must throw PERMISSION_DENIED");
        }

        // Release lock
        client.unlockObject(setKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL: only the owner can perform operations, all others are blocked")
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_global_lock";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Owner acquires GLOBAL LOCK
        LockStatus lock = client.lockObject(setKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should acquire GLOBAL LOCK");

        // Owner can read
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());

        // Owner can write
        Integer added = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(2L, "item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Owner should add element");

        // Intruder cannot read
        try {
            client.streamOrderedSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Intruder must not have read access under GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Must throw PERMISSION_DENIED");
        }

        // Intruder cannot write
        try {
            client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(3L, "item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Intruder must not have write access under GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Must throw PERMISSION_DENIED");
        }

        assertDenied(client.unlockObject(setKey, INTRUDER_CLIENT_ID));

        // Release lock by owner
        client.unlockObject(setKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("unlockObject: only the lock owner can release it")
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unlock_owner";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Owner acquires WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should acquire WRITE_LOCK");

        // Intruder tries to unlock
        assertDenied(client.unlockObject(setKey, INTRUDER_CLIENT_ID));

        // Owner releases the lock
        LockStatus validUnlock = client.unlockObject(setKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock, "Owner should successfully unlock");
    }

    // =========================================================================
    // 11. GET ELEMENT BY WEIGHT OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getElementWithWeight returns element by specific weight")
    void testGetElementWithWeight() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_weight";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3"),
                op(40L, "item4"),
                op(50L, "item5")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Get element with weight 30
        Payload result = client.getElementWithWeight(setKey, hint, 30).get();
        assertNotNull(result);
        assertEquals("item3", str(result));
    }

    @Test
    @DisplayName("getElementWithWeight returns any matching element if multiple have the same weight")
    void testGetElementWithWeightMultipleSameWeight() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_weight_multi";
        List<OrderedPayload> initialData = List.of(
                op(20L, "item1"),
                op(20L, "item2"),
                op(20L, "item3"),
                op(40L, "item4")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Get element with weight 20
        Payload result = client.getElementWithWeight(setKey, hint, 20).get();
        assertNotNull(result);
        assertTrue(str(result).equals("item1") || str(result).equals("item2") || str(result).equals("item3"));
    }

    @Test
    @DisplayName("getElementAtPosition throws NOT_FOUND if weight does not exist")
    void testGetElementAtPositionNotFound() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_pos_notfound";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        try {
            client.getElementAtPosition(setKey, hint, 100).get();
            fail("getElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Must throw NOT_FOUND error");
        }
    }

    @Test
    @DisplayName("getAndRemoveElementWithWeight returns and removes element by weight")
    void testGetAndRemoveElementWithWeight() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_remove_weight";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Get and remove element with weight 20
        Payload result = client.getAndRemoveElementWithWeight(setKey, hint, 20).get();
        assertNotNull(result);
        assertEquals("item2", str(result));

        // Verify element is removed
        List<OrderedPayload> remaining = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, remaining.size(), "Ordered set must contain 2 elements");
    }

    @Test
    @DisplayName("getAndRemoveElementAtPosition throws NOT_FOUND if weight does not exist")
    void testGetAndRemoveElementAtPositionNotFound() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_remove_pos_notfound";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        try {
            client.getAndRemoveElementAtPosition(setKey, hint, 100).get();
            fail("getAndRemoveElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Must throw NOT_FOUND error");
        }
    }

    // =========================================================================
    // 12. UNSUPPORTED METHODS FOR ORDERED SET
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to ordered set must throw errors")
    void testUnsupportedMethodsForOrderedSet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unsupported";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // getHead - not applicable to ordered set
        try {
            client.getHead(setKey, hint).get();
            fail("getHead must throw error for ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Must throw INTERNAL error");
        }

        // getFront - not applicable to ordered set
        try {
            client.getHead(setKey, hint).get();
            fail("getFront must throw error for ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Must throw INTERNAL error");
        }

        // getTail - not applicable to ordered set
        try {
            client.getTail(setKey, hint).get();
            fail("getTail must throw error for ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Must throw INTERNAL error");
        }

        // streamElementInRangeUnordered - not applicable to ordered set
        try {
            client.streamElementInRangeUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, ContainerType.ORDERED_SET, 0, 10, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("streamElementInRangeUnordered must throw error for ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode(), "Must throw INVALID_ARGUMENT");
        } catch (IllegalArgumentException e) {
            assertEquals("Unsupported container type for stream operation: ORDERED_SET", e.getMessage());
        }
    }

    // =========================================================================
    // 13. ADDITIONAL EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("streamOrderedSet with non-existent ordered set throws NOT_FOUND")
    void testStreamOrderedSetNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_nonexist";
        KeyHintData hint = KeyHintData.of(1, 1);

        try {
            client.streamOrderedSet(setKey, hint).get();
            fail("streamOrderedSet for a non-existent set must throw error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Must throw NOT_FOUND");
        }
    }

    @Test
    @DisplayName("Unlocking an expired lock returns OK")
    void testUnlockOnExpiredLock() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unlock_expired";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Acquire lock with TTL = 1 second
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock, "Lock must be acquired");

        Thread.sleep(1500);

        // Try to release expired lock
        LockStatus unlock = client.unlockObject(setKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock, "Releasing an expired lock must return OK");
    }

    @Test
    @DisplayName("streamElementInRangeOrdered includes endWeight")
    void testStreamElementInRangeOrderedEndWeightIncluded() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_end_included";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 10L, 30L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Should have 3 elements, endWeight 30 must be included");
        assertEquals(30L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrdered includes startWeight")
    void testStreamElementInRangeOrderedStartWeightIncluded() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_start_included";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 10L, 30L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Should have 3 elements, startWeight 10 must be included");
        assertEquals(10L, result.get(0).getOrder());
    }

    @Test
    @DisplayName("removeElementAtPosition removes range including endPos inclusively")
    void testRemoveElementAtPositionEndPosIncluded() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_pos_end_included";
        List<OrderedPayload> initialData = List.of(
                op(10L, "item1"),
                op(20L, "item2"),
                op(30L, "item3"),
                op(40L, "item4")
        );

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        Boolean removed = client.removeElementAtPosition(setKey, hint, 20, 30).get();
        assertTrue(removed, "Elements must be removed");

        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set must contain 2 elements");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(40L, result.get(1).getOrder());
    }


    // =========================================================================
    // 14. LOCK EXPIRATION & TIMEOUT TESTS
    // =========================================================================

    @Test
    @DisplayName("WRITE_LOCK automatically expires after specified duration allowing another client to acquire it")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_lock_expire_write";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Owner acquires WRITE_LOCK with a short TTL of 1 second
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock, "Owner should acquire WRITE_LOCK");

        // Intruder is initially blocked
        try {
            client.streamOrderedSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Intruder must be blocked while lock is active");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Wait for the lock TTL to expire
        Thread.sleep(3000);

        // After expiration, intruder should successfully acquire WRITE_LOCK or read/write without permission denied
        LockStatus intruderLock = client.lockObject(setKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock, "Intruder should successfully acquire the lock after expiration");

        // Verify intruder can now perform operations
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, INTRUDER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());

        client.unlockObject(setKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL lock automatically expires after duration, removing access restrictions")
    void testGlobalLockExpiration() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_lock_expire_global";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Owner acquires GLOBAL lock with a short TTL of 1 second
        LockStatus lock = client.lockObject(setKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock, "Owner should acquire GLOBAL lock");

        // Wait for expiration
        Thread.sleep(2000);

        // Verify that the lock is effectively gone and another client can acquire a write lock
        LockStatus newLock = client.lockObject(setKey, LockType.WRITE_LOCK, SECONDARY_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock, "Secondary client should acquire lock after global lock has expired");

        client.unlockObject(setKey, SECONDARY_CLIENT_ID).get();
    }

    @Test
    @DisplayName("READ_LOCK prevents write operations from any client")
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_read_lock_writes";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Client acquires READ_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Client should acquire READ_LOCK");

        // Reading under READ_LOCK should succeed
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, DEFAULT_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());

        // Writing (adding elements) under READ_LOCK must fail with PERMISSION_DENIED
        try {
            client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(2L, "item2")), DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Writing under READ_LOCK must not be allowed");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Must throw PERMISSION_DENIED for write attempt under read lock");
        }

        // Release lock
        client.unlockObject(setKey, DEFAULT_CLIENT_ID).get();
    }

    @Test
    @DisplayName("READ_LOCK prevents write operations from any client and auto releases after timeout")
    void testReadLockBlocksWritesWithTimeout() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_read_lock_writes";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));

        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);

        // Client acquires READ_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(5)).get();
        assertEquals(LockStatus.OK, lock, "Client should acquire READ_LOCK");

        // Reading under READ_LOCK should succeed
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, DEFAULT_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());

        Thread.sleep(6000);
        // Writing (adding elements) under READ_LOCK must fail with PERMISSION_DENIED

        Integer item2 = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8),
                                                    hint,
                                                    List.of(op(2L, "item2")),
                                                    DEFAULT_CLIENT_ID,
                                                    Duration.ofSeconds(30)).get();

        // Release lock
        LockStatus lockStatus = client.unlockObject(setKey, DEFAULT_CLIENT_ID).get();
        assertEquals(LockStatus.OK, lockStatus, "Client should release READ");
    }
}