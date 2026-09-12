package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
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

        List<Payload> streamed = client.streamList(key).get();

        assertNotNull(streamed);
        Assertions.assertEquals(0, streamed.size());
    }

    @Test
    @DisplayName("createList: create list with initial data")
    void testCreateListWithInitialData() throws ExecutionException, InterruptedException {
        String key = "createListWithInitialData" + UUID.randomUUID();

        Assertions.assertNotNull(client.createList(key, List.of(
                Payload.of("v1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("v2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("v3".getBytes(StandardCharsets.UTF_8))
        )).get());

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
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
        List<Payload> payloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            payloads.add(Payload.of(createLargePayload(PAYLOAD_SIZE)));
        }

        CompletableFuture<KeyHintData> future = client.createList(key, null, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        List<Payload> streamed = client.streamList(key, hint, 0, TIMEOUT).get();
        Assertions.assertEquals(LARGE_ELEMENT_COUNT, streamed.size());
    }

    // =========================================================================
    // 2. STREAM LIST
    // =========================================================================

    @Test
    @DisplayName("streamList: get contents of the entire list")
    void testStreamListContents() throws ExecutionException, InterruptedException {
        String key = "streamListContents" + UUID.randomUUID();
        List<String> expectedValues = Arrays.asList("a", "b", "c", "d", "e");

        client.createList(key, expectedValues.stream()
                .map(v -> Payload.of(v.getBytes(StandardCharsets.UTF_8)))
                .toList()).get();

        List<String> actual = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(expectedValues, actual);
    }

    // =========================================================================
    // 3. GET AND REMOVE FRONT
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveFront: extract and remove front of the list")
    void testGetAndRemoveFront() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveFront" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveFront(key).get();
        assertNotNull(removed);
        Assertions.assertEquals("first", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("second", remaining.get(0));
        Assertions.assertEquals("third", remaining.get(1));
    }

    @Test
    @DisplayName("getAndRemoveFront: on single-element list (edge: empty after)")
    void testGetAndRemoveFrontOnSingleElementList() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveFrontSingle" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("only".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveFront(key).get();
        assertNotNull(removed);
        Assertions.assertEquals("only", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<Payload> remaining = client.streamList(key).get();
        Assertions.assertEquals(0, remaining.size());
    }

    // =========================================================================
    // 4. GET FRONT / GET HEAD
    // =========================================================================

    @Test
    @DisplayName("getFront/getHead: get list head without removal")
    void testGetFrontAndHead() throws ExecutionException, InterruptedException {
        String key = "getFrontHead" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("tail".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload head = client.getHead(key).get();

        assertNotNull(head);
        Assertions.assertEquals("head", new String(head.getValue(), StandardCharsets.UTF_8));

        // List should remain unchanged
        List<String> all = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();
        Assertions.assertEquals(2, all.size());
        Assertions.assertEquals("head", all.get(0));
        Assertions.assertEquals("tail", all.get(1));
    }

    @Test
    @DisplayName("getFront: on single-element list (edge case)")
    void testGetFrontOnSingleElementList() throws ExecutionException, InterruptedException {
        String key = "getFrontSingle" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("only".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload front = client.getHead(key).get();
        assertNotNull(front);
        Assertions.assertEquals("only", new String(front.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. GET TAIL
    // =========================================================================

    @Test
    @DisplayName("getTail: get list tail")
    void testGetTail() throws ExecutionException, InterruptedException {
        String key = "getTail" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("middle".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload tail = client.getTail(key).get();
        assertNotNull(tail);
        Assertions.assertEquals("last", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getTail: on single-element list (head == tail)")
    void testGetTailOnSingleElementList() throws ExecutionException, InterruptedException {
        String key = "getTailSingle" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("only".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload head = client.getHead(key).get();
        Payload tail = client.getTail(key).get();

        assertNotNull(head);
        assertNotNull(tail);
        Assertions.assertEquals("only", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("only", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. GET AND REMOVE TAIL
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveTail: extract and remove tail of the list")
    void testGetAndRemoveTail() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveTail" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveTail(key).get();
        assertNotNull(removed);
        Assertions.assertEquals("last", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("first", remaining.get(0));
        Assertions.assertEquals("second", remaining.get(1));
    }

    @Test
    @DisplayName("getAndRemoveTail: on single-element list (list becomes empty)")
    void testGetAndRemoveTailOnSingleElementList() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveTailSingle" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("only".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveTail(key).get();
        assertNotNull(removed);
        Assertions.assertEquals("only", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<Payload> remaining = client.streamList(key).get();
        Assertions.assertEquals(0, remaining.size());
    }

    // =========================================================================
    // 7. GET ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition: get element at position")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getElementAtPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("pos0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos2".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload pos0 = client.getElementAtPosition(key, 0).get();
        Payload pos1 = client.getElementAtPosition(key, 1).get();
        Payload pos2 = client.getElementAtPosition(key, 2).get();

        assertNotNull(pos0);
        assertNotNull(pos1);
        assertNotNull(pos2);
        Assertions.assertEquals("pos0", new String(pos0.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("pos2", new String(pos2.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getElementAtPosition: position 0 (edge case)")
    void testGetElementAtPositionZero() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionZero" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload pos0 = client.getElementAtPosition(key, 0).get();
        assertNotNull(pos0);
        Assertions.assertEquals("first", new String(pos0.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getElementAtPosition: last position (edge case)")
    void testGetElementAtPositionLast() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionLast" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload last = client.getElementAtPosition(key, 1).get();
        assertNotNull(last);
        Assertions.assertEquals("second", new String(last.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 8. GET AND REMOVE ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveElementAtPosition: get and remove element at position")
    void testGetAndRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementAtPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("a".getBytes(StandardCharsets.UTF_8)),
                Payload.of("b".getBytes(StandardCharsets.UTF_8)),
                Payload.of("c".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveElementAtPosition(key, null, 1).get();
        assertNotNull(removed);
        Assertions.assertEquals("b", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("a", remaining.get(0));
        Assertions.assertEquals("c", remaining.get(1));
    }

    @Test
    @DisplayName("getAndRemoveElementAtPosition: position 0 (edge case)")
    void testGetAndRemoveElementAtPositionFirst() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementAtPositionFirst" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveElementAtPosition(key, null, 0).get();
        assertNotNull(removed);
        Assertions.assertEquals("first", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("second", remaining.get(0));
        Assertions.assertEquals("third", remaining.get(1));
    }

    // =========================================================================
    // 9. ADD ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("addElementToPosition: add element to position")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String key = "addElementToPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPosition(key, null,
                                                    List.of(Payload.of("middle".getBytes(StandardCharsets.UTF_8))), 1).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("first", results.get(0));
        Assertions.assertEquals("middle", results.get(1));
        Assertions.assertEquals("last", results.get(2));
    }

    @Test
    @DisplayName("addElementToPosition: insert at position == size (edge case)")
    void testAddElementToPositionAtEnd() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAtEnd" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPosition(key, null,
                                                    List.of(Payload.of("last".getBytes(StandardCharsets.UTF_8))), 2).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("first", results.get(0));
        Assertions.assertEquals("second", results.get(1));
        Assertions.assertEquals("last", results.get(2));
    }

    // =========================================================================
    // 10. REMOVE ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition: remove element at position")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPosition" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8))
        )).get();

        Boolean removed = client.removeElementAtPosition(key, null, 1, 1).get();
        assertTrue(removed);

        List<String> remaining = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("0", remaining.get(0));
        Assertions.assertEquals("2", remaining.get(1));
    }

    // =========================================================================
    // 11. ADD ELEMENT AT POSITION BEFORE
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore: insert element before value")
    void testAddElementToPositionBefore() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionBefore" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pivot".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPositionBefore(key,
                                                          List.of(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                                                          Payload.of("pivot".getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("head", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
        Assertions.assertEquals("pivot", results.get(2));
    }

    @Test
    @DisplayName("addElementToPositionBefore: insert before first element (edge case)")
    void testAddElementToPositionBeforeFirst() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionBeforeFirst" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPositionBefore(key,
                                                          List.of(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                                                          Payload.of("first".getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("inserted", results.get(0));
        Assertions.assertEquals("first", results.get(1));
        Assertions.assertEquals("second", results.get(2));
    }

    // =========================================================================
    // 12. ADD ELEMENT AT POSITION AFTER
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionAfter: insert element after value")
    void testAddElementToPositionAfter() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAfter" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("tail".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPositionAfter(key,
                                                         List.of(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                                                         Payload.of("head".getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("head", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
        Assertions.assertEquals("tail", results.get(2));
    }

    @Test
    @DisplayName("addElementToPositionAfter: insert after last element (edge case)")
    void testAddElementToPositionAfterLast() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAfterLast" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPositionAfter(key,
                                                         List.of(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                                                         Payload.of("last".getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("first", results.get(0));
        Assertions.assertEquals("last", results.get(1));
        Assertions.assertEquals("inserted", results.get(2));
    }

    // =========================================================================
    // 13. STREAM ELEMENT IN RANGE UNORDERED
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeUnordered: get elements by position range")
    void testStreamElementInRangeUnordered() throws ExecutionException, InterruptedException {
        String key = "streamElementInRangeUnordered" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("3".getBytes(StandardCharsets.UTF_8)),
                Payload.of("4".getBytes(StandardCharsets.UTF_8)),
                Payload.of("5".getBytes(StandardCharsets.UTF_8))
        )).get();

        List<Payload> range = client.streamElementInRangeUnordered(key, null, ContainerType.LIST, 2, 4).get();
        assertNotNull(range);
        Assertions.assertEquals(3, range.size());
        Assertions.assertEquals("2", new String(range.get(0).getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("3", new String(range.get(1).getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("4", new String(range.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("streamElementInRangeUnordered: full range [0, size-1] (edge case)")
    void testStreamElementInRangeUnorderedFullRange() throws ExecutionException, InterruptedException {
        String key = "streamElementInRangeFullRange" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("3".getBytes(StandardCharsets.UTF_8)),
                Payload.of("4".getBytes(StandardCharsets.UTF_8))
        )).get();

        List<Payload> range = client.streamElementInRangeUnordered(key, null, ContainerType.LIST, 0, 4).get();
        assertNotNull(range);
        Assertions.assertEquals(5, range.size());
    }

    // =========================================================================
    // 14. GET SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize: get list size")
    void testGetListSize() throws ExecutionException, InterruptedException {
        String key = "getListSize" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("a".getBytes(StandardCharsets.UTF_8)),
                Payload.of("b".getBytes(StandardCharsets.UTF_8)),
                Payload.of("c".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer size = client.getSize(key).get();
        Assertions.assertEquals(3, size);

        client.addElementToTail(key, null, List.of(Payload.of("d".getBytes(StandardCharsets.UTF_8)))).get();

        size = client.getSize(key).get();
        Assertions.assertEquals(4, size);
    }

    @Test
    @DisplayName("remove: delete non-existent list (edge case)")
    void testRemoveNonExistentList() throws ExecutionException, InterruptedException {
        String key = "removeNonExistentList" + UUID.randomUUID();

        try {
            client.remove(key).get();
            Assertions.fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 15. LARGE LIST STRICT CONTENT
    // =========================================================================

    @Test
    @DisplayName("createList: strict verification of order and values of a large list")
    void testCreateLargeListStrictContent() throws ExecutionException, InterruptedException {
        byte[] key = ("strictLargeList" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        List<Payload> expectedPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] payload = new byte[PAYLOAD_SIZE];
            byte headerMarker = (byte) (i % 127);
            for (int j = 0; j < PAYLOAD_SIZE; j++) {
                payload[j] = (byte) ((j + headerMarker) % 256);
            }
            expectedPayloads.add(Payload.of(payload));
        }

        CompletableFuture<KeyHintData> future = client.createList(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        List<Payload> actualPayloads = client.streamList(key, hint, 0, TIMEOUT).get();
        Assertions.assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(expectedPayloads.get(i).getValue(), actualPayloads.get(i).getValue(),
                              "List mismatch at index: " + i);
        }
    }

    // =========================================================================
    // 16. REMOVE LIST
    // =========================================================================

    @Test
    @DisplayName("remove: delete list")
    void testRemoveList() throws ExecutionException, InterruptedException {
        String key = "removeList" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("item2".getBytes(StandardCharsets.UTF_8))
        )).get();

        List<String> before = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();
        Assertions.assertEquals(2, before.size());

        Boolean removed = client.remove(key).get();
        assertTrue(removed);

        try {
            client.streamList(key).get();
            Assertions.fail("Expected NOT_FOUND after remove");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 17. SET TTL
    // =========================================================================

    @Test
    @DisplayName("setTtl: set TTL on list")
    void testSetTtlOnList() throws ExecutionException, InterruptedException {
        String key = "setTtlList" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("a".getBytes(StandardCharsets.UTF_8)),
                Payload.of("b".getBytes(StandardCharsets.UTF_8))
        )).get();

        Boolean setTtl = client.setTtl(key, null, 5000L).get();
        assertTrue(setTtl);

        Long ttl = client.getTtl(key).get();
        assertNotNull(ttl);
        Assertions.assertTrue(ttl > 0);
    }

    // =========================================================================
    // 18. TTL EXPIRY
    // =========================================================================

    @Test
    @DisplayName("TTL: list should be deleted after TTL expiration")
    void testListTtlExpiry() throws ExecutionException, InterruptedException {
        String key = "ttlExpiryList" + UUID.randomUUID();

        client.createList(key, List.of(
                Payload.of("temp".getBytes(StandardCharsets.UTF_8))
        )).get();

        Boolean setTtl = client.setTtl(key, null, 2000L).get();
        assertTrue(setTtl);

        Thread.sleep(2500);

        try {
            client.streamList(key).get();
            Assertions.fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 18. LOCK LIST
    // =========================================================================

    @Test
    @DisplayName("lockObject: list WRITE_LOCK locking")
    void testWriteLockOnList() throws ExecutionException, InterruptedException {
        String key = "writeLockList" + UUID.randomUUID();
        int ownerId = 1;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject: list READ_LOCK locking")
    void testReadLockOnList() throws ExecutionException, InterruptedException {
        String key = "readLockList" + UUID.randomUUID();
        int ownerId = 2;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.READ_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject: list GLOBAL locking")
    void testGlobalLockOnList() throws ExecutionException, InterruptedException {
        String key = "globalLockList" + UUID.randomUUID();
        int ownerId = 3;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject: list NO_LOCK locking")
    void testNoLockOnList() throws ExecutionException, InterruptedException {
        String key = "noLockList" + UUID.randomUUID();
        int ownerId = 4;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.NO_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject READ_LOCK: read works, write is blocked")
    void testReadLockAllowsReadButBlocksWrite() throws ExecutionException, InterruptedException {
        String key = "readLockReadWrite" + UUID.randomUUID();
        int readerId = 20;
        int writerId = 21;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Reader locks for reading
        LockStatus lockStatus = client.lockObject(key, LockType.READ_LOCK, readerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Reading works
        Payload readData = client.getHead(key).get();
        assertNotNull(readData);

        // Writer cannot add an element
        assertDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));

        // Unlock
        LockStatus unlockStatus = client.unlockObject(key, readerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject WRITE_LOCK: owner reads and writes, others cannot do anything")
    void testWriteLockOwnerReadsWritesOthersBlocked() throws ExecutionException, InterruptedException {
        String key = "writeLockOwnerReadWrite" + UUID.randomUUID();
        int ownerId = 30;
        int otherId = 31;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Owner locks for writing
        LockStatus lockStatus = client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads
        Payload readData = client.getHead(key, ownerId).get();
        assertNotNull(readData);

        // Owner writes
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), ownerId).get();
        Assertions.assertTrue(added >= 0);

        // Others cannot write
        assertDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), otherId));

        // Others cannot read
        assertDenied(client.getHead(key, otherId));

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject READ_LOCK: owner reads, others read too, nobody writes")
    void testReadLockEveryoneReadsNobodyWrites() throws ExecutionException, InterruptedException {
        String key = "readLockEveryoneReads" + UUID.randomUUID();
        int readerId = 40;
        int otherReaderId = 41;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Reader locks for reading
        LockStatus lockStatus = client.lockObject(key, LockType.READ_LOCK, readerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads
        Payload readData = client.getHead(key).get();
        assertNotNull(readData);

        // Others read in parallel
        Payload otherReadData = client.getHead(key, otherReaderId).get();
        assertNotNull(otherReadData);

        // Nobody can write (neither owner nor others)
        assertDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));
        assertDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));

        LockStatus unlockStatus = client.unlockObject(key, readerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject GLOBAL: owner works, all others blocked")
    void testGlobalLockOwnerWorksOthersBlocked() throws ExecutionException, InterruptedException {
        String key = "globalLockOwnerWorks" + UUID.randomUUID();
        int ownerId = 50;
        int otherId = 51;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Owner locks GLOBAL
        LockStatus lockStatus = client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads
        Payload readData = client.getHead(key, ownerId).get();
        assertNotNull(readData);

        // Owner writes
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), ownerId).get();
        Assertions.assertTrue(added >= 0);

        // Others cannot read
        assertDenied(client.getHead(key, otherId));

        // Others cannot write
        assertDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject WRITE_LOCK: owner reads and writes, others cannot do anything")
    void testWriteLockOnlyOwnerWorks() throws ExecutionException, InterruptedException {
        String key = "writeLockOnlyOwnerWorks" + UUID.randomUUID();
        int ownerId = 60;
        int otherId = 61;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Owner locks for writing
        LockStatus lockStatus = client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads - works
        Payload readData = client.getHead(key, ownerId).get();
        assertNotNull(readData);

        Payload tailData = client.getTail(key, ownerId).get();
        assertNotNull(tailData);

        List<Payload> streamData = client.streamList(key, ownerId).get();
        assertNotNull(streamData);

        // Owner writes - works
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8))), ownerId).get();
        Assertions.assertTrue(added >= 0);

        // Others cannot read
        assertDenied(client.getHead(key, otherId));

        // Others cannot write
        assertDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), otherId));

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject GLOBAL: owner deletes via remove")
    void testGlobalLockOwnerCanRemove() throws ExecutionException, InterruptedException {
        String key = "globalLockOwnerRemove" + UUID.randomUUID();
        int ownerId = 70;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner deletes
        Boolean removed = client.remove(key, ownerId).get();
        assertTrue(removed);

        // After deletion, unlock throws NOT_FOUND
        try {
            client.unlockObject(key, ownerId).get();
            Assertions.fail("Expected NOT_FOUND - object deleted");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }

    }

    @Test
    @DisplayName("lockObject: CANT_LOCK when locked by another client")
    void testCantLockByOtherClient() throws ExecutionException, InterruptedException {
        String key = "cantLockByOtherClient" + UUID.randomUUID();
        int ownerId = 5;
        int intruderId = 6;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        assertDenied(client.lockObject(key, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(60)));


        // Owner can unlock
        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    // =========================================================================
    // 19. UNLOCK LIST
    // =========================================================================

    @Test
    @DisplayName("unlockObject: owner can unlock list")
    void testUnlockByOwner() throws ExecutionException, InterruptedException {
        String key = "unlockByOwnerList" + UUID.randomUUID();
        int ownerId = 10;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("unlockObject: non-owner cannot unlock list")
    void testCantUnlockByNonOwner() throws ExecutionException, InterruptedException {
        String key = "cantUnlockByNonOwner" + UUID.randomUUID();
        int ownerId = 11;
        int intruderId = 12;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        assertDenied(client.unlockObject(key, intruderId));


        // Owner can unlock
        LockStatus ownerUnlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, ownerUnlockStatus);
    }

    @Test
    @DisplayName("unlockObject: unlock after list deleted (NOT_FOUND)")
    void testUnlockAfterListDeleted() throws ExecutionException, InterruptedException {
        String key = "unlockAfterListDeleted" + UUID.randomUUID();
        int ownerId = 13;

        client.createList(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        // Owner deletes the list
        Boolean removed = client.remove(key, ownerId).get();
        assertTrue(removed);

        // Unlock returns NOT_FOUND
        try {
            client.unlockObject(key, ownerId).get();
            Assertions.fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    // =========================================================================
    // 20. ERROR CASES
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveFront on non-existent list")
    void testGetAndRemoveFrontOnMissingList() {
        String key = "getAndRemoveFrontMissing" + UUID.randomUUID();

        try {
            client.getAndRemoveFront(key).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("getAndRemoveTail on non-existent list")
    void testGetAndRemoveTailOnMissingList() {
        String key = "getAndRemoveTailMissing" + UUID.randomUUID();

        try {
            client.getAndRemoveTail(key).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("getElementAtPosition on non-existent list")
    void testGetElementAtPositionOnMissingList() {
        String key = "getElementAtPositionMissing" + UUID.randomUUID();

        try {
            client.getElementAtPosition(key, 0).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("getAndRemoveElementAtPosition on non-existent list")
    void testGetAndRemoveElementAtPositionOnMissingList() {
        String key = "getAndRemoveElementAtPositionMissing" + UUID.randomUUID();

        try {
            client.getAndRemoveElementAtPosition(key, null, 0).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("addElementToPosition on non-existent list")
    void testAddElementToPositionOnMissingList() {
        String key = "addElementToPositionMissing" + UUID.randomUUID();

        try {
            client.addElementToPosition(key, null, List.of(Payload.of("x".getBytes(StandardCharsets.UTF_8))), 0).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("removeElementAtPosition on non-existent list")
    void testRemoveElementAtPositionOnMissingList() {
        String key = "removeElementAtPositionMissing" + UUID.randomUUID();

        try {
            client.removeElementAtPosition(key, null, 0, 1).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("addElementToPositionBefore on non-existent list")
    void testAddElementToPositionBeforeOnMissingList() {
        String key = "addElementToPositionBeforeMissing" + UUID.randomUUID();

        try {
            client.addElementToPositionBefore(key,
                                              List.of(Payload.of("x".getBytes(StandardCharsets.UTF_8))),
                                              Payload.of("pivot".getBytes(StandardCharsets.UTF_8))).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("remove on non-existent list")
    void testRemoveOnMissingList() {
        String key = "removeMissingList" + UUID.randomUUID();

        try {
            client.remove(key).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("lockObject on non-existent list")
    void testLockOnMissingList() {
        String key = "lockMissingList" + UUID.randomUUID();

        try {
            client.lockObject(key, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    // =========================================================================
    // REMOVE FROM CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer removes element by value and returns removed count")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String vecKey = "list_remove"+UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of( "item1".getBytes(StandardCharsets.UTF_8)), Payload.of( "item2".getBytes(StandardCharsets.UTF_8)), Payload.of( "item3".getBytes(StandardCharsets.UTF_8)));

        KeyHintData hint = client.createList(vecKey, initialData).get();
        assertNotNull(hint);

        Integer removed = client.removeFromContainer(vecKey.getBytes(StandardCharsets.UTF_8), hint, "item1".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(1, removed, "1 element should be removed");

        List<Payload> result = client.streamList(vecKey, hint).get();
        assertEquals(2, result.size(), "Ordered set must contain 2 elements");
    }

    @Test
    @DisplayName("removeFromContainer can remove multiple duplicates with same key but different weights")
    void testRemoveFromContainerMultipleDuplicates() throws ExecutionException, InterruptedException {
        String vecKey = "list_remove_multi"+UUID.randomUUID();


        List<Payload> initialData = List.of(
                Payload.of( "item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of( "item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of( "item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of( "item3".getBytes(StandardCharsets.UTF_8))
        );
        KeyHintData hint = client.createList(vecKey, initialData).get();
        assertNotNull(hint);

        // Remove all elements matching key "item1" (3 items)
        Integer removed = client.removeFromContainer(vecKey.getBytes(StandardCharsets.UTF_8), hint, "item1".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(3, removed, "3 elements with the same key must be removed");

        List<Payload> result = client.streamList(vecKey, hint).get();
        assertEquals(1, result.size(), "Ordered set must contain 1 element");
        assertEquals("item3", new String(result.get(0).getValue()));
    }

    @Test
    @DisplayName("removeFromContainer returns 0 if element does not exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String vecKey = "list_remove_nonexistent"+UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of( "item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of( "item2".getBytes(StandardCharsets.UTF_8))
        );

        KeyHintData hint = client.createList(vecKey, initialData).get();
        assertNotNull(hint);

        Integer removed = client.removeFromContainer(vecKey.getBytes(StandardCharsets.UTF_8), hint, "item123".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");
    }
}