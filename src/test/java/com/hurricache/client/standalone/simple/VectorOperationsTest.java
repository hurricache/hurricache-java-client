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

public class VectorOperationsTest extends TestBase {

    private void assertLockDenied(java.util.concurrent.CompletableFuture<?> future) {
        try {
            future.get();
            Assertions.fail("Expected PERMISSION_DENIED - Access Denied by Lock");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
            Assertions.assertTrue(cause.getStatus().getDescription().contains("Access Denied by Lock"),
                                  "Expected 'Access Denied by Lock' but got: " + cause.getStatus().getDescription());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    // =========================================================================
    // 1. CREATE VECTOR
    // =========================================================================

    @Test
    @DisplayName("createVector: create empty vector")
    void testCreateEmptyVector() throws ExecutionException, InterruptedException {
        String key = "createEmptyVector" + UUID.randomUUID();

        Assertions.assertNotNull(client.createVector(key, List.of()).get());

        List<Payload> streamed = client.streamVector(key).get();

        assertNotNull(streamed);
        Assertions.assertEquals(0, streamed.size());
    }

    @Test
    @DisplayName("createVector: create vector with initial data")
    void testCreateVectorWithInitialData() throws ExecutionException, InterruptedException {
        String key = "createVectorWithInitialData" + UUID.randomUUID();

        Assertions.assertNotNull(client.createVector(key, List.of(
                Payload.of("v1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("v2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("v3".getBytes(StandardCharsets.UTF_8))
        )).get());

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("v1", results.get(0));
        Assertions.assertEquals("v2", results.get(1));
        Assertions.assertEquals("v3", results.get(2));
    }

    @Test
    @DisplayName("createVector: create large vector with chunking")
    void testCreateLargeVectorWithChunking() throws ExecutionException, InterruptedException {
        byte[] key = ("largeVectorKey" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        List<Payload> payloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            payloads.add(Payload.of(createLargePayload(PAYLOAD_SIZE)));
        }

        CompletableFuture<KeyHintData> future = client.createVector(key, null, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        List<Payload> streamed = client.streamVector(key, hint, 0, TIMEOUT).get();
        Assertions.assertEquals(LARGE_ELEMENT_COUNT, streamed.size());
    }

    // =========================================================================
    // 2. STREAM VECTOR
    // =========================================================================

    @Test
    @DisplayName("streamVector: get contents of the entire vector")
    void testStreamVectorContents() throws ExecutionException, InterruptedException {
        String key = "streamVectorContents" + UUID.randomUUID();
        List<String> expectedValues = Arrays.asList("a", "b", "c", "d", "e");

        client.createVector(key, expectedValues.stream()
                .map(v -> Payload.of(v.getBytes(StandardCharsets.UTF_8)))
                .toList()).get();

        List<String> actual = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(expectedValues, actual);
    }

    // =========================================================================
    // 3. GET AND REMOVE FRONT
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveFront: extract and remove front of the vector")
    void testGetAndRemoveFront() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveFront" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveFront(key).get();
        assertNotNull(removed);
        Assertions.assertEquals("first", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("second", remaining.get(0));
        Assertions.assertEquals("third", remaining.get(1));
    }

    // =========================================================================
    // 4. GET FRONT / GET HEAD
    // =========================================================================

    @Test
    @DisplayName("getFront/getHead: get vector head without removal")
    void testGetFrontAndHead() throws ExecutionException, InterruptedException {
        String key = "getFrontHead" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("tail".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload head = client.getHead(key).get();
        Payload front = client.getFront(key).get();

        assertNotNull(head);
        assertNotNull(front);
        Assertions.assertEquals("head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("head", new String(front.getValue(), StandardCharsets.UTF_8));

        // Vector should remain unchanged
        List<String> all = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();
        Assertions.assertEquals(2, all.size());
        Assertions.assertEquals("head", all.get(0));
        Assertions.assertEquals("tail", all.get(1));
    }

    // =========================================================================
    // 5. GET TAIL
    // =========================================================================

    @Test
    @DisplayName("getTail: get vector tail")
    void testGetTail() throws ExecutionException, InterruptedException {
        String key = "getTail" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("middle".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload tail = client.getTail(key).get();
        assertNotNull(tail);
        Assertions.assertEquals("last", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. GET AND REMOVE TAIL
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveTail: extract and remove tail of the vector")
    void testGetAndRemoveTail() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveTail" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveTail(key).get();
        assertNotNull(removed);
        Assertions.assertEquals("last", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("first", remaining.get(0));
        Assertions.assertEquals("second", remaining.get(1));
    }

    // =========================================================================
    // 7. GET ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition: get element at position")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getElementAtPosition" + UUID.randomUUID();

        client.createVector(key, List.of(
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

    // =========================================================================
    // 8. GET AND REMOVE ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveElementAtPosition: get and remove element at position")
    void testGetAndRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementAtPosition" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("a".getBytes(StandardCharsets.UTF_8)),
                Payload.of("b".getBytes(StandardCharsets.UTF_8)),
                Payload.of("c".getBytes(StandardCharsets.UTF_8))
        )).get();

        Payload removed = client.getAndRemoveElementAtPosition(key, null, 1).get();
        assertNotNull(removed);
        Assertions.assertEquals("b", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<String> remaining = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, remaining.size());
        Assertions.assertEquals("a", remaining.get(0));
        Assertions.assertEquals("c", remaining.get(1));
    }

    // =========================================================================
    // 9. ADD ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("addElementToPosition: add element to position")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String key = "addElementToPosition" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPosition(key, null,
                                                    List.of(Payload.of("middle".getBytes(StandardCharsets.UTF_8))), 1).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("first", results.get(0));
        Assertions.assertEquals("middle", results.get(1));
        Assertions.assertEquals("last", results.get(2));
    }

    // =========================================================================
    // 10. REMOVE ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition: remove element at position")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPosition" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8))
        )).get();

        Boolean removed = client.removeElementAtPosition(key, null, 1, 1).get();
        assertTrue(removed);

        List<String> remaining = client.streamVector(key).get()
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

        client.createVector(key, List.of(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pivot".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPositionBefore(key,
                                                          List.of(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                                                          Payload.of("pivot".getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("head", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
        Assertions.assertEquals("pivot", results.get(2));
    }

    // =========================================================================
    // 12. ADD ELEMENT AT POSITION AFTER
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionAfter: insert element after value")
    void testAddElementToPositionAfter() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAfter" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("tail".getBytes(StandardCharsets.UTF_8))
        )).get();

        Integer added = client.addElementToPositionAfter(key,
                                                         List.of(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                                                         Payload.of("head".getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertTrue(added >= 0);

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("head", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
        Assertions.assertEquals("tail", results.get(2));
    }

    // =========================================================================
    // 13. STREAM ELEMENT IN RANGE UNORDERED
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeUnordered: get elements by position range")
    void testStreamElementInRangeUnordered() throws ExecutionException, InterruptedException {
        String key = "streamElementInRangeUnordered" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("3".getBytes(StandardCharsets.UTF_8)),
                Payload.of("4".getBytes(StandardCharsets.UTF_8)),
                Payload.of("5".getBytes(StandardCharsets.UTF_8))
        )).get();

        List<Payload> range = client.streamElementInRangeUnordered(key, null, ContainerType.VECTOR, 2, 4).get();
        assertNotNull(range);
        Assertions.assertEquals(3, range.size());
        Assertions.assertEquals("2", new String(range.get(0).getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("3", new String(range.get(1).getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("4", new String(range.get(2).getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // GET SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize: get vector size")
    void testGetVectorSize() throws ExecutionException, InterruptedException {
        String key = "getVectorSize" + UUID.randomUUID();

        client.createVector(key, List.of(
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

    // =========================================================================
    // LARGE VECTOR STRICT CONTENT
    // =========================================================================

    @Test
    @DisplayName("createVector: strict verification of order and values of a large vector")
    void testCreateLargeVectorStrictContent() throws ExecutionException, InterruptedException {
        byte[] key = ("strictLargeVector" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        List<Payload> expectedPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] payload = new byte[PAYLOAD_SIZE];
            byte headerMarker = (byte) (i % 127);
            for (int j = 0; j < PAYLOAD_SIZE; j++) {
                payload[j] = (byte) ((j + headerMarker) % 256);
            }
            expectedPayloads.add(Payload.of(payload));
        }

        CompletableFuture<KeyHintData> future = client.createVector(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        List<Payload> actualPayloads = client.streamVector(key, hint, 0, TIMEOUT).get();
        Assertions.assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(expectedPayloads.get(i).getValue(), actualPayloads.get(i).getValue(),
                              "Vector mismatch at index: " + i);
        }
    }

    // =========================================================================
    // 14. REMOVE VECTOR
    // =========================================================================

    @Test
    @DisplayName("remove: delete vector")
    void testRemoveVector() throws ExecutionException, InterruptedException {
        String key = "removeVector" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("item2".getBytes(StandardCharsets.UTF_8))
        )).get();

        List<String> before = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();
        Assertions.assertEquals(2, before.size());

        Boolean removed = client.remove(key).get();
        assertTrue(removed);

        try {
            client.streamVector(key).get();
            Assertions.fail("Expected NOT_FOUND after remove");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 15. SET TTL
    // =========================================================================

    @Test
    @DisplayName("setTtl: set TTL on vector")
    void testSetTtlOnVector() throws ExecutionException, InterruptedException {
        String key = "setTtlVector" + UUID.randomUUID();

        client.createVector(key, List.of(
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
    // 16. TTL EXPIRY
    // =========================================================================

    @Test
    @DisplayName("TTL: vector should be deleted after TTL expiration")
    void testVectorTtlExpiry() throws ExecutionException, InterruptedException {
        String key = "ttlExpiryVector" + UUID.randomUUID();

        client.createVector(key, List.of(
                Payload.of("temp".getBytes(StandardCharsets.UTF_8))
        )).get();

        Boolean setTtl = client.setTtl(key, null, 2000L).get();
        assertTrue(setTtl);

        Thread.sleep(2500);

        try {
            client.streamVector(key).get();
            Assertions.fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 17. LOCK VECTOR
    // =========================================================================

    @Test
    @DisplayName("lockObject: vector WRITE_LOCK locking")
    void testWriteLockOnVector() throws ExecutionException, InterruptedException {
        String key = "writeLockVector" + UUID.randomUUID();
        int ownerId = 1;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject: vector READ_LOCK locking")
    void testReadLockOnVector() throws ExecutionException, InterruptedException {
        String key = "readLockVector" + UUID.randomUUID();
        int ownerId = 2;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.READ_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject: vector GLOBAL locking")
    void testGlobalLockOnVector() throws ExecutionException, InterruptedException {
        String key = "globalLockVector" + UUID.randomUUID();
        int ownerId = 3;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        LockStatus lockStatus = client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject: vector NO_LOCK locking")
    void testNoLockOnVector() throws ExecutionException, InterruptedException {
        String key = "noLockVector" + UUID.randomUUID();
        int ownerId = 4;

        client.createVector(key, List.of(
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

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Reader locks for reading
        LockStatus lockStatus = client.lockObject(key, LockType.READ_LOCK, readerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Reading works
        Payload readData = client.getFront(key).get();
        assertNotNull(readData);

        // Writer cannot add an element
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));

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

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Owner locks for writing
        LockStatus lockStatus = client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads
        Payload readData = client.getFront(key, ownerId).get();
        assertNotNull(readData);

        // Owner writes
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), ownerId).get();
        Assertions.assertTrue(added >= 0);

        // Others cannot write
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), otherId));

        // Others cannot read
        assertLockDenied(client.getFront(key, otherId));

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject READ_LOCK: owner reads, others read too, nobody writes")
    void testReadLockEveryoneReadsNobodyWrites() throws ExecutionException, InterruptedException {
        String key = "readLockEveryoneReads" + UUID.randomUUID();
        int readerId = 40;
        int otherReaderId = 41;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Reader locks for reading
        LockStatus lockStatus = client.lockObject(key, LockType.READ_LOCK, readerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads
        Payload readData = client.getFront(key).get();
        assertNotNull(readData);

        // Others read in parallel
        Payload otherReadData = client.getFront(key, otherReaderId).get();
        assertNotNull(otherReadData);

        // Nobody can write (neither owner nor others)
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));

        LockStatus unlockStatus = client.unlockObject(key, readerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject GLOBAL: owner works, all others blocked")
    void testGlobalLockOwnerWorksOthersBlocked() throws ExecutionException, InterruptedException {
        String key = "globalLockOwnerWorks" + UUID.randomUUID();
        int ownerId = 50;
        int otherId = 51;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Owner locks GLOBAL
        LockStatus lockStatus = client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads
        Payload readData = client.getFront(key, ownerId).get();
        assertNotNull(readData);

        // Owner writes
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), ownerId).get();
        Assertions.assertTrue(added >= 0);

        // Others cannot read
        assertLockDenied(client.getFront(key, otherId));

        // Others cannot write
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8)))));

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject WRITE_LOCK: owner reads and writes, others cannot do anything")
    void testWriteLockOnlyOwnerWorks() throws ExecutionException, InterruptedException {
        String key = "writeLockOnlyOwnerWorks" + UUID.randomUUID();
        int ownerId = 60;
        int otherId = 61;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        // Owner locks for writing
        LockStatus lockStatus = client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // Owner reads - works
        Payload readData = client.getFront(key, ownerId).get();
        assertNotNull(readData);

        Payload tailData = client.getTail(key, ownerId).get();
        assertNotNull(tailData);

        List<Payload> streamData = client.streamVector(key, ownerId).get();
        assertNotNull(streamData);

        // Owner writes - works
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8))), ownerId).get();
        Assertions.assertTrue(added >= 0);

        // Others cannot read
        assertLockDenied(client.getFront(key, otherId));

        // Others cannot write
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("write".getBytes(StandardCharsets.UTF_8))), otherId));

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject GLOBAL: owner deletes via remove")
    void testGlobalLockOwnerCanRemove() throws ExecutionException, InterruptedException {
        String key = "globalLockOwnerRemove" + UUID.randomUUID();
        int ownerId = 70;

        client.createVector(key, List.of(
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
    void testCantLockOnVectorByOtherClient() throws ExecutionException, InterruptedException {
        String key = "cantLockVector" + UUID.randomUUID();
        int ownerId = 5;
        int intruderId = 6;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        assertLockDenied(client.lockObject(key, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(60)));


        // Owner can unlock
        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    // =========================================================================
    // 18. UNLOCK VECTOR
    // =========================================================================

    @Test
    @DisplayName("unlockObject: owner can unlock vector")
    void testUnlockVectorByOwner() throws ExecutionException, InterruptedException {
        String key = "unlockByOwnerVector" + UUID.randomUUID();
        int ownerId = 10;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        LockStatus unlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("unlockObject: non-owner cannot unlock vector")
    void testCantUnlockVectorByNonOwner() throws ExecutionException, InterruptedException {
        String key = "cantUnlockVector" + UUID.randomUUID();
        int ownerId = 11;
        int intruderId = 12;

        client.createVector(key, List.of(
                Payload.of("data".getBytes(StandardCharsets.UTF_8))
        )).get();

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        assertLockDenied(client.unlockObject(key, intruderId));


        // Owner can unlock
        LockStatus ownerUnlockStatus = client.unlockObject(key, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, ownerUnlockStatus);
    }

    // =========================================================================
    // ERROR CASES
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveFront on non-existent vector")
    void testGetAndRemoveFrontOnMissingVector() {
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
    @DisplayName("getAndRemoveTail on non-existent vector")
    void testGetAndRemoveTailOnMissingVector() {
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
    @DisplayName("remove on non-existent vector")
    void testRemoveOnMissingVector() {
        String key = "removeMissingVector" + UUID.randomUUID();

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
    @DisplayName("lockObject on non-existent vector")
    void testLockOnMissingVector() {
        String key = "lockMissingVector" + UUID.randomUUID();

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
}