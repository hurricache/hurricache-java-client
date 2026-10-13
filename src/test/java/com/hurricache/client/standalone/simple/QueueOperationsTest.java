package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueueOperationsTest extends TestBase {

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    // =========================================================================
    // 1. CREATE QUEUE
    // =========================================================================

    @Test
    @DisplayName("createQueue: create empty queue")
    void testCreateEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "createEmptyQueue" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of()).get();
        assertNotNull(hint);

        Variant head = client.getHead(key).get();
        assertEquals(0, head.bytesValue().length);
    }

    @Test
    @DisplayName("createQueue: create queue with initial data")
    void testCreateQueueWithInitialData() throws ExecutionException, InterruptedException {
        String key = "createQueueWithInitialData" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();
        assertNotNull(hint);

        Thread.sleep(500);

        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("createQueue: create large queue with chunking")
    void testCreateLargeQueueWithChunking() throws ExecutionException, InterruptedException {
        byte[] key = ("largeQueueKey" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        List<Variant> variants = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            variants.add(Variant.ofBytes(createLargePayload(PAYLOAD_SIZE)));
        }

        KeyHintData hint = client.createQueue(key, null, variants, getTestTtl(), 0, TIMEOUT).get();
        assertNotNull(hint);

        Thread.sleep(500);

        int count = 0;
        while (true) {
            Variant p = client.getAndDeleteHead(key, hint, 0, TIMEOUT).get();
            if (p.bytesValue().length == 0) break;
            count++;
        }
        assertEquals(LARGE_ELEMENT_COUNT, count);
    }

    // =========================================================================
    // 2. EMPTY QUEUE EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("empty queue: getHead returns empty Variant")
    void testGetHeadOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueueGetHead" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

        Thread.sleep(500);

        Variant head = client.getHead(key).get();
        assertEquals(0, head.bytesValue().length);
    }

    @Test
    @DisplayName("empty queue: getAndDeleteHead returns empty Variant")
    void testGetAndDeleteHeadOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueueGetAndDelete" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

        Thread.sleep(500);

        Variant removed = client.getAndDeleteHead(key).get();
        assertEquals(0, removed.bytesValue().length);
    }

    // =========================================================================
    // 3. GET AND DELETE HEAD
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteHead: dequeue from multi-element queue")
    void testGetAndDeleteHead() throws ExecutionException, InterruptedException {
        String key = "getAndDeleteHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant removed = client.getAndDeleteHead(key).get();
        assertEquals("first", new String(removed.bytesValue(), StandardCharsets.UTF_8));

        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteHead: dequeue from single-element queue")
    void testGetAndDeleteHeadOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "singleElementQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Variant removed = client.getAndDeleteHead(key).get();
        assertEquals("only", new String(removed.bytesValue(), StandardCharsets.UTF_8));

        Variant empty = client.getHead(key).get();
        assertEquals(0, empty.bytesValue().length);
    }

    // =========================================================================
    // 4. GET HEAD
    // =========================================================================

    @Test
    @DisplayName("getHead: return front element without removal")
    void testGetHead() throws ExecutionException, InterruptedException {
        String key = "getHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant head = client.getHead(key).get();

        assertEquals("first", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant stillThere = client.getAndDeleteHead(key).get();
        assertEquals("first", new String(stillThere.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead: single element edge case")
    void testGetHeadOnSingleElementQueue() throws ExecutionException, InterruptedException {
        String key = "singleElementHead" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Variant head = client.getHead(key).get();

        assertEquals("only", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. GET TAIL
    // =========================================================================

    @Test
    @DisplayName("getTail: get tail from multi-element queue")
    void testGetTail() throws ExecutionException, InterruptedException {
        String key = "getTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant tail = client.getTail(key).get();
        assertEquals("third", new String(tail.bytesValue(), StandardCharsets.UTF_8));

        // Queue unchanged
        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getTail: single element = head == tail")
    void testGetTailOnSingleElementQueue() throws ExecutionException, InterruptedException {
        String key = "singleElementTail" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Variant head = client.getHead(key).get();
        Variant tail = client.getTail(key).get();

        assertEquals("only", new String(head.bytesValue(), StandardCharsets.UTF_8));
        assertEquals("only", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. GET AND DELETE TAIL
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteTail: pop back from multi-element queue")
    void testGetAndDeleteTail() throws ExecutionException, InterruptedException {
        String key = "getAndDeleteTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Variant removed = client.getAndDeleteTail(key).get();
        assertEquals("third", new String(removed.bytesValue(), StandardCharsets.UTF_8));

        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteTail: single element, queue becomes empty")
    void testGetAndDeleteTailOnSingleElementQueue() throws ExecutionException, InterruptedException {
        String key = "singleElementTailRemove" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Variant removed = client.getAndDeleteTail(key).get();
        assertEquals("only", new String(removed.bytesValue(), StandardCharsets.UTF_8));

        Variant empty = client.getHead(key).get();
        assertEquals(0, empty.bytesValue().length);
    }

    // =========================================================================
    // 7. ADD ELEMENT TO TAIL
    // =========================================================================

    @Test
    @DisplayName("addElementToTail: add element to existing queue")
    void testAddElementToTail() throws ExecutionException, InterruptedException {
        String key = "addElementToTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToTail(key, null, List.of(Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8)))).get();
        assertEquals(1, added);

        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 8. ADD ELEMENT TO HEAD
    // =========================================================================

    @Test
    @DisplayName("addElementToHead: prepend element to queue")
    void testAddElementToHead() throws ExecutionException, InterruptedException {
        String key = "addElementToHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToHead(key, null, List.of(Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)))).get();
        assertEquals(1, added);

        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToHead: multiple prepends, last prepend = head")
    void testAddElementToHeadMultiple() throws ExecutionException, InterruptedException {
        String key = "addElementToHeadMultiple" + UUID.randomUUID();

        client.createQueue(key, List.of()).get();

        Thread.sleep(500);

        client.addElementToHead(key, null, List.of(Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)))).get();
        client.addElementToHead(key, null, List.of(Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)))).get();
        client.addElementToHead(key, null, List.of(Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8)))).get();

        assertEquals("third", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 9. DELETE HEAD
    // =========================================================================

    @Test
    @DisplayName("deleteHead: remove without return, verify next element")
    void testDeleteHead() throws ExecutionException, InterruptedException {
        String key = "deleteHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer deleted = client.deleteHead(key).get();
        assertTrue(deleted > 0);

        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 10. DELETE TAIL
    // =========================================================================

    @Test
    @DisplayName("deleteTail: remove without return, verify new tail")
    void testDeleteTail() throws ExecutionException, InterruptedException {
        String key = "deleteTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer deleted = client.deleteTail(key).get();
        assertTrue(deleted > 0);

        assertEquals("first", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndDeleteHead(key).get().bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteTail: single element, queue becomes empty")
    void testDeleteTailOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "deleteTailSingle" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Integer deleted = client.deleteTail(key).get();
        assertTrue(deleted > 0);

        Variant empty = client.getHead(key).get();
        assertEquals(0, empty.bytesValue().length);
    }

    // =========================================================================
    // 11. GET SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize: returns 0 for queue (not supported)")
    void testGetSizeReturnsZero() throws ExecutionException, InterruptedException {
        String key = "getSizeQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Variant.ofBytes("first".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    // =========================================================================
    // 12. REMOVE QUEUE
    // =========================================================================

    @Test
    @DisplayName("removeQueue: remove queue, verify NOT_FOUND on next operation")
    void testRemoveQueue() throws ExecutionException, InterruptedException {
        String key = "removeQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        boolean removed = client.remove(key).get();
        assertTrue(removed);

        try {
            client.getHead(key).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("removeQueue: remove non-existent queue, verify NOT_FOUND")
    void testRemoveNonExistentQueue() throws ExecutionException, InterruptedException {
        String key = "removeNonExistent" + UUID.randomUUID();

        try {
            client.remove(key).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 13. SET TTL / GET TTL
    // =========================================================================

    @Test
    @DisplayName("setTtl/getTtl: set TTL and verify")
    void testSetTtlAndGetTtlOnQueue() throws ExecutionException, InterruptedException {
        String key = "ttlQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        boolean setTtl = client.setTtl(key, null, 5000).get();
        assertTrue(setTtl);

        Long ttl = client.getTtl(key).get();
        assertTrue(ttl > 0);
    }

    // =========================================================================
    // 14. TTL EXPIRY
    // =========================================================================

    @Test
    @DisplayName("queueTtlExpiry: queue expires after TTL")
    void testQueueTtlExpiry() throws ExecutionException, InterruptedException {
        String key = "ttlExpiryQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        client.setTtl(key, null, 2000).get();

        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(5000));

        try {
            System.out.println(client.getTtl(key).get());
            client.getHead(key).get();
            Assertions.fail("Expected NOT_FOUND error after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 15. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("lock: WRITE_LOCK acquire and unlock")
    void testWriteLockOnQueue() throws ExecutionException, InterruptedException {
        String key = "writeLockQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 5;
        LockStatus lockRes = client.lockObject(key, LockType.WRITE_LOCK, clientId, Duration.ofSeconds(60)).get();
        assertEquals(LockStatus.OK, lockRes);

        LockStatus unlockRes = client.unlockObject(key, clientId).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    @Test
    @DisplayName("lock: READ_LOCK acquire and unlock")
    void testReadLockOnQueue() throws ExecutionException, InterruptedException {
        String key = "readLockQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(key, LockType.READ_LOCK, 0, Duration.ofSeconds(60)).get();
        assertEquals(LockStatus.OK, lockRes);

        LockStatus unlockRes = client.unlockObject(key, 0).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    @Test
    @DisplayName("lock: GLOBAL_LOCK acquire and unlock")
    void testGlobalLockOnQueue() throws ExecutionException, InterruptedException {
        String key = "globalLockQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(key, LockType.GLOBAL, 1, Duration.ofSeconds(60)).get();
        assertEquals(LockStatus.OK, lockRes);

        LockStatus unlockRes = client.unlockObject(key, 1).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    @Test
    @DisplayName("lock: NO_LOCK acquire and unlock")
    void testNoLockOnQueue() throws ExecutionException, InterruptedException {
        String key = "noLockQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(key, LockType.NO_LOCK, 0, Duration.ofSeconds(60)).get();
        assertEquals(LockStatus.OK, lockRes);

        LockStatus unlockRes = client.unlockObject(key, 0).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    // =========================================================================
    // 16. UNSUPPORTED METHODS ERROR CASES
    // =========================================================================

    @Test
    @DisplayName("unsupported: getElementAtPosition returns INTERNAL")
    void testGetElementAtPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.getElementAtPosition(key.getBytes(StandardCharsets.UTF_8), null, 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: getAndDeleteElementAtPosition returns INTERNAL")
    void testGetAndDeleteElementAtPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue2" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.getAndDeleteElementAtPosition(key.getBytes(StandardCharsets.UTF_8), null, 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: addElementToPosition returns INTERNAL")
    void testAddElementToPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue3" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.addElementToPosition(key.getBytes(StandardCharsets.UTF_8), null, List.of(Variant.ofBytes("new".getBytes(StandardCharsets.UTF_8))), 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: deleteElementAtPosition returns INTERNAL")
    void testDeleteElementAtPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue4" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.deleteElementAtPosition(key.getBytes(StandardCharsets.UTF_8), null, 0, 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: addElementToPositionBefore returns INTERNAL")
    void testAddElementToPositionBeforeOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue5" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.addElementToPositionBefore(key.getBytes(StandardCharsets.UTF_8), null, List.of(Variant.ofBytes("new".getBytes(StandardCharsets.UTF_8))), Variant.ofBytes("pivot".getBytes(StandardCharsets.UTF_8)), 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: addElementToPositionAfter returns INTERNAL")
    void testAddElementToPositionAfterOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue6" + UUID.randomUUID();

        client.createQueue(key, List.of(Variant.ofBytes("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.addElementToPositionAfter(key.getBytes(StandardCharsets.UTF_8), null, List.of(Variant.ofBytes("new".getBytes(StandardCharsets.UTF_8))), Variant.ofBytes("pivot".getBytes(StandardCharsets.UTF_8)), 0, TIMEOUT));
    }

    // =========================================================================
    // 17. MISSING QUEUE ERROR CASES
    // =========================================================================

    @Test
    @DisplayName("missing queue: getAndDeleteHead returns NOT_FOUND")
    void testGetAndDeleteHeadOnMissingQueue() throws ExecutionException, InterruptedException {
        String key = "missingQueueGetAndDelete" + UUID.randomUUID();

        try {
            client.getAndDeleteHead(key).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("missing queue: getTail returns NOT_FOUND")
    void testGetTailOnMissingQueue() throws ExecutionException, InterruptedException {
        String key = "missingQueueGetTail" + UUID.randomUUID();

        try {
            client.getTail(key).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
}