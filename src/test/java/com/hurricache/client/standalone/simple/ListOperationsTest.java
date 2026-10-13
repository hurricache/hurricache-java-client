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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListOperationsTest extends TestBase {

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    // =========================================================================
    // 1. CREATE LIST
    // =========================================================================

    @Test
    @DisplayName("createList: create empty list")
    void testCreateEmptyList() throws ExecutionException, InterruptedException {
        String key = "createEmptyList" + UUID.randomUUID();

        Assertions.assertNotNull(client.createList(key, List.of()).get());

        List<Variant> streamed = client.streamList(key).get();

        assertNotNull(streamed);
        Assertions.assertEquals(0, streamed.size());
    }

    @Test
    @DisplayName("createList: create list with initial data")
    void testCreateListWithInitialData() throws ExecutionException, InterruptedException {
        String key = "createListWithInitialData" + UUID.randomUUID();

        Assertions.assertNotNull(client.createList(key, List.of(
                Variant.ofBytes("v1".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("v2".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("v3".getBytes(StandardCharsets.UTF_8))
        )).get());

        List<String> results = client.streamList(key).get()
                .stream()
                .map(v -> v.isBytes() ? new String(v.bytesValue(), StandardCharsets.UTF_8) : v.toString())
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("v1", results.get(0));
        Assertions.assertEquals("v2", results.get(1));
        Assertions.assertEquals("v3", results.get(2));
    }

    @Test
    @DisplayName("createList: create large list with chunking")
    void testCreateLargeListWithChunking() throws ExecutionException, InterruptedException {
        byte[] key = ("largeListKey" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        List<Variant> variants = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            variants.add(Variant.ofBytes(createLargePayload(PAYLOAD_SIZE)));
        }

        CompletableFuture<KeyHintData> future = client.createList(key, null, variants, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        List<Variant> streamed = client.streamList(key, hint, 0, TIMEOUT).get();
        Assertions.assertEquals(LARGE_ELEMENT_COUNT, streamed.size());
    }

    // =========================================================================
    // 2. STREAM LIST
    // =========================================================================

    @Test
    @DisplayName("streamList: get contents of the entire list")
    void testStreamListContents() throws ExecutionException, InterruptedException {
        String key = "streamListContents" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("a".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("b".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("c".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        List<Variant> results = client.streamList(key).get();

        Assertions.assertEquals(3, results.size());
        assertEquals("a", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("b", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("c", new String(results.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("streamList: empty list returns empty list")
    void testStreamEmptyList() throws ExecutionException, InterruptedException {
        String key = "streamEmptyList" + UUID.randomUUID();

        client.createList(key, List.of()).get();

        Thread.sleep(500);

        List<Variant> results = client.streamList(key).get();

        assertNotNull(results);
        Assertions.assertEquals(0, results.size());
    }

    // =========================================================================
    // 3. ADD ELEMENT TO TAIL
    // =========================================================================

    @Test
    @DisplayName("addElementToTail: append to list")
    void testAddElementToTail() throws ExecutionException, InterruptedException {
        String key = "addElementToTail" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToTail(key, null, List.of(
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        assertEquals(2, added);

        List<Variant> results = client.streamList(key).get();
        assertEquals(3, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(results.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 4. ADD ELEMENT TO HEAD
    // =========================================================================

    @Test
    @DisplayName("addElementToHead: prepend to list")
    void testAddElementToHead() throws ExecutionException, InterruptedException {
        String key = "addElementToHead" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToHead(key, null, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8))
        )).get();

        assertEquals(1, added);

        List<Variant> results = client.streamList(key).get();
        assertEquals(2, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. ADD ELEMENT TO POSITION
    // =========================================================================

    @Test
    @DisplayName("addElementToPosition: insert at specific position")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String key = "addElementToPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToPosition(key, null, List.of(
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        ), 1, 0, TIMEOUT).get();

        assertEquals(1, added);

        List<Variant> results = client.streamList(key).get();
        assertEquals(3, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(results.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. ADD ELEMENT BY PIVOT
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore: insert before pivot")
    void testAddElementToPositionBefore() throws ExecutionException, InterruptedException {
        String key = "addElementBefore" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToPositionBefore(key, null, List.of(
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        ), Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8)), 0, TIMEOUT).get();

        assertEquals(1, added);

        List<Variant> results = client.streamList(key).get();
        assertEquals(3, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(results.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionAfter: insert after pivot")
    void testAddElementToPositionAfter() throws ExecutionException, InterruptedException {
        String key = "addElementAfter" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToPositionAfter(key, null, List.of(
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        ), Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)), 0, TIMEOUT).get();

        assertEquals(1, added);

        List<Variant> results = client.streamList(key).get();
        assertEquals(3, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(results.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 7. GET ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition: retrieve element by index")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getElementAtPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant element = client.getElementAtPosition(key, null, 1, 0, TIMEOUT).get();

        assertNotNull(element);
        assertEquals("second", new String(element.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getElementAtPosition: first element (index 0)")
    void testGetElementAtPositionFirst() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionFirst" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("only".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant element = client.getElementAtPosition(key, null, 0, 0, TIMEOUT).get();

        assertNotNull(element);
        assertEquals("only", new String(element.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 8. GET AND DELETE ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteElementAtPosition: remove and return element by index")
    void testGetAndDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getAndDeleteElementAtPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant removed = client.getAndDeleteElementAtPosition(key, null, 1, 0, TIMEOUT).get();

        assertNotNull(removed);
        assertEquals("second", new String(removed.bytesValue(), StandardCharsets.UTF_8));

        List<Variant> results = client.streamList(key).get();
        assertEquals(2, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(results.get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 9. DELETE ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("deleteElementAtPosition: remove element without return")
    void testDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "deleteElementAtPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer deleted = client.deleteElementAtPosition(key, null, 1, 1, 0, TIMEOUT).get();

        assertTrue(deleted > 0);

        List<Variant> results = client.streamList(key).get();
        assertEquals(2, results.size());
    }

    // =========================================================================
    // 10. DELETE HEAD/TAIL
    // =========================================================================

    @Test
    @DisplayName("deleteHead: remove first element")
    void testDeleteHead() throws ExecutionException, InterruptedException {
        String key = "deleteHead" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer deleted = client.deleteHead(key).get();
        assertTrue(deleted > 0);

        List<Variant> results = client.streamList(key).get();
        assertEquals(1, results.size());
        assertEquals("second", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteTail: remove last element")
    void testDeleteTail() throws ExecutionException, InterruptedException {
        String key = "deleteTail" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer deleted = client.deleteTail(key).get();
        assertTrue(deleted > 0);

        List<Variant> results = client.streamList(key).get();
        assertEquals(1, results.size());
        assertEquals("first", new String(results.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 11. GET HEAD/TAIL
    // =========================================================================

    @Test
    @DisplayName("getHead: get first element without removal")
    void testGetHead() throws ExecutionException, InterruptedException {
        String key = "getHead" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant head = client.getHead(key).get();
        assertEquals("first", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getTail: get last element without removal")
    void testGetTail() throws ExecutionException, InterruptedException {
        String key = "getTail" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant tail = client.getTail(key).get();
        assertEquals("second", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 12. REMOVE LIST
    // =========================================================================

    @Test
    @DisplayName("removeList: remove list entirely")
    void testRemoveList() throws ExecutionException, InterruptedException {
        String key = "removeList" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        boolean removed = client.remove(key).get();
        assertTrue(removed);

        try {
            client.streamList(key).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 13. GET SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize: returns list size")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "getSizeList" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("a".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("b".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("c".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 14. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("lock: WRITE_LOCK on list")
    void testWriteLockOnList() throws ExecutionException, InterruptedException {
        String key = "writeLockList" + UUID.randomUUID();

        client.createList(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 5;
        LockStatus lockRes = client.lockObject(key, LockType.WRITE_LOCK, clientId, Duration.ofSeconds(60)).get();
        assertEquals(LockStatus.OK, lockRes);

        LockStatus unlockRes = client.unlockObject(key, clientId).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    // =========================================================================
    // 15. STREAM ELEMENT IN RANGE
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeUnordered: get subset of list")
    void testStreamElementInRangeUnordered() throws ExecutionException, InterruptedException {
        String key = "streamElementInRange" + UUID.randomUUID();

        client.createList(key, List.of(
                Variant.ofBytes("a".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("b".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("c".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("d".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        List<Variant> range = client.streamElementInRangeUnordered(key, null, ContainerType.LIST, 1, 2, 0, TIMEOUT).get();

        assertEquals(2, range.size());
        assertEquals("b", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("c", new String(range.get(1).bytesValue(), StandardCharsets.UTF_8));
    }
}