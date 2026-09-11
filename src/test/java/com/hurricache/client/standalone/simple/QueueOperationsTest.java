package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueueOperationsTest extends TestBase {

    private void assertLockDenied(CompletableFuture<?> future) {
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

    private void assertUnsupportedMethod(CompletableFuture<?> future) {
        try {
            future.get();
            Assertions.fail("Expected INTERNAL error for unsupported method");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
            Assertions.assertTrue(cause.getStatus().getDescription().contains("Key not found or type is not correct"),
                                  "Expected 'Key not found or type is not correct' but got: " + cause.getStatus().getDescription());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

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

        Payload head = client.getHead(key).get();
        assertEquals(0, head.getValue().length);
    }

    @Test
    @DisplayName("createQueue: create queue with initial data")
    void testCreateQueueWithInitialData() throws ExecutionException, InterruptedException {
        String key = "createQueueWithInitialData" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();
        assertNotNull(hint);

        Thread.sleep(500);

        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("createQueue: create large queue with chunking")
    void testCreateLargeQueueWithChunking() throws ExecutionException, InterruptedException {
        byte[] key = ("largeQueueKey" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        List<Payload> payloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            payloads.add(Payload.of(createLargePayload(PAYLOAD_SIZE)));
        }

        KeyHintData hint = client.createQueue(key, null, payloads, getTestTtl(), 0, TIMEOUT).get();
        assertNotNull(hint);

        Thread.sleep(500);

        int count = 0;
        while (true) {
            Payload p = client.getAndRemoveFront(key, hint, 0, TIMEOUT).get();
            if (p.getValue().length == 0) break;
            count++;
        }
        assertEquals(LARGE_ELEMENT_COUNT, count);
    }

    // =========================================================================
    // 2. EMPTY QUEUE EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("empty queue: getHead returns empty Payload")
    void testGetHeadOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueueGetHead" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

        Thread.sleep(500);

        Payload head = client.getHead(key).get();
        assertEquals(0, head.getValue().length);
    }

    @Test
    @DisplayName("empty queue: getAndRemoveFront returns empty Payload")
    void testGetAndRemoveFrontOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueueGetAndRemove" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

        Thread.sleep(500);

        Payload removed = client.getAndRemoveFront(key).get();
        assertEquals(0, removed.getValue().length);
    }

    // =========================================================================
    // 3. GET AND REMOVE FRONT
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveFront: dequeue from multi-element queue")
    void testGetAndRemoveFront() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveFront" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Payload removed = client.getAndRemoveFront(key).get();
        assertEquals("first", new String(removed.getValue(), StandardCharsets.UTF_8));

        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndRemoveFront: dequeue from single-element queue")
    void testGetAndRemoveFrontOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "singleElementQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Payload removed = client.getAndRemoveFront(key).get();
        assertEquals("only", new String(removed.getValue(), StandardCharsets.UTF_8));

        Payload empty = client.getHead(key).get();
        assertEquals(0, empty.getValue().length);
    }

    // =========================================================================
    // 4. GET FRONT / GET HEAD
    // =========================================================================

    @Test
    @DisplayName("getFront/getHead: both return same front element")
    void testGetFrontAndHead() throws ExecutionException, InterruptedException {
        String key = "getFrontHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Payload front = client.getFront(key).get();
        Payload head = client.getHead(key).get();

        assertEquals("first", new String(front.getValue(), StandardCharsets.UTF_8));
        assertEquals("first", new String(head.getValue(), StandardCharsets.UTF_8));

        Payload stillThere = client.getAndRemoveFront(key).get();
        assertEquals("first", new String(stillThere.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getFront/getHead: single element edge case")
    void testGetFrontOnSingleElementQueue() throws ExecutionException, InterruptedException {
        String key = "singleElementFront" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Payload front = client.getFront(key).get();
        Payload head = client.getHead(key).get();

        assertEquals("only", new String(front.getValue(), StandardCharsets.UTF_8));
        assertEquals("only", new String(head.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. GET TAIL
    // =========================================================================

    @Test
    @DisplayName("getTail: get tail from multi-element queue")
    void testGetTail() throws ExecutionException, InterruptedException {
        String key = "getTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Payload tail = client.getTail(key).get();
        assertEquals("third", new String(tail.getValue(), StandardCharsets.UTF_8));

        // Queue unchanged
        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getTail: single element = head == tail")
    void testGetTailOnSingleElementQueue() throws ExecutionException, InterruptedException {
        String key = "singleElementTail" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Payload head = client.getHead(key).get();
        Payload tail = client.getTail(key).get();

        assertEquals("only", new String(head.getValue(), StandardCharsets.UTF_8));
        assertEquals("only", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. GET AND REMOVE TAIL
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveTail: pop back from multi-element queue")
    void testGetAndRemoveTail() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Payload removed = client.getAndRemoveTail(key).get();
        assertEquals("third", new String(removed.getValue(), StandardCharsets.UTF_8));

        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndRemoveTail: single element, queue becomes empty")
    void testGetAndRemoveTailOnSingleElementQueue() throws ExecutionException, InterruptedException {
        String key = "singleElementTailRemove" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        Payload removed = client.getAndRemoveTail(key).get();
        assertEquals("only", new String(removed.getValue(), StandardCharsets.UTF_8));

        Payload empty = client.getHead(key).get();
        assertEquals(0, empty.getValue().length);
    }

    // =========================================================================
    // 7. ADD ELEMENT TO TAIL
    // =========================================================================

    @Test
    @DisplayName("addElementToTail: add element to existing queue")
    void testAddElementToTail() throws ExecutionException, InterruptedException {
        String key = "addElementToTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToTail(key, null, List.of(Payload.of("third".getBytes(StandardCharsets.UTF_8)))).get();
        assertEquals(1, added);

        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 8. ADD ELEMENT TO HEAD
    // =========================================================================

    @Test
    @DisplayName("addElementToHead: prepend element to queue")
    void testAddElementToHead() throws ExecutionException, InterruptedException {
        String key = "addElementToHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        Integer added = client.addElementToHead(key, null, List.of(Payload.of("first".getBytes(StandardCharsets.UTF_8)))).get();
        assertEquals(1, added);

        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToHead: multiple prepends, last prepend = head")
    void testAddElementToHeadMultiple() throws ExecutionException, InterruptedException {
        String key = "addElementToHeadMultiple" + UUID.randomUUID();

        client.createQueue(key, List.of()).get();

        Thread.sleep(500);

        client.addElementToHead(key, null, List.of(Payload.of("first".getBytes(StandardCharsets.UTF_8)))).get();
        client.addElementToHead(key, null, List.of(Payload.of("second".getBytes(StandardCharsets.UTF_8)))).get();
        client.addElementToHead(key, null, List.of(Payload.of("third".getBytes(StandardCharsets.UTF_8)))).get();

        assertEquals("third", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 9. REMOVE HEAD
    // =========================================================================

    @Test
    @DisplayName("removeHead: remove without return, verify next element")
    void testRemoveHead() throws ExecutionException, InterruptedException {
        String key = "removeHead" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        boolean removed = client.removeHead(key).get();
        assertTrue(removed);

        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 10. REMOVE TAIL
    // =========================================================================

    @Test
    @DisplayName("removeTail: remove without return, verify new tail")
    void testRemoveTail() throws ExecutionException, InterruptedException {
        String key = "removeTail" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500);

        boolean removed = client.removeTail(key).get();
        assertTrue(removed);

        assertEquals("first", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(client.getAndRemoveFront(key).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("removeTail: single element, queue becomes empty")
    void testRemoveTailOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "removeTailSingle" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("only".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        boolean removed = client.removeTail(key).get();
        assertTrue(removed);

        Payload empty = client.getHead(key).get();
        assertEquals(0, empty.getValue().length);
    }

    // =========================================================================
    // 11. GET SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize: returns 0 for queue (not supported)")
    void testGetSizeReturnsZero() throws ExecutionException, InterruptedException {
        String key = "getSizeQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
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

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

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

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

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

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

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

    // 15a. Basic lock types

    @Test
    @DisplayName("lock: WRITE_LOCK acquire and unlock")
    void testWriteLockOnQueue() throws ExecutionException, InterruptedException {
        String key = "writeLockQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

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

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

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

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

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

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(key, LockType.NO_LOCK, 0, Duration.ofSeconds(60)).get();
        assertEquals(LockStatus.OK, lockRes);

        LockStatus unlockRes = client.unlockObject(key, 0).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    // 15b. READ_LOCK behavior

    @Test
    @DisplayName("lock: READ_LOCK allows read, blocks write")
    void testReadLockAllowsReadBlocksWrite() throws ExecutionException, InterruptedException {
        String key = "readLockBehavior" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 2;
        client.lockObject(key, LockType.READ_LOCK, clientId, Duration.ofSeconds(60)).get();

        // Owner can read
        Payload head = client.getHead(key,clientId).get();
        assertNotNull(head);

        // Owner can write (READ_LOCK only blocks other clients)
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))),clientId).get();
        assertEquals(1, added);

        Payload payload = client.getHead(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT).get();
        assertNotNull(payload);

        // Intruder cannot write
        assertLockDenied(client.addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("intruder".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));
    }

    @Test
    @DisplayName("lock: READ_LOCK blocks all write operations for others")
    void testReadLockBlocksAllWriteOperations() throws ExecutionException, InterruptedException {
        String keyStr = "readLockWriteOps" + UUID.randomUUID();
        byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);

        client.createQueue(keyStr, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        client.lockObject(keyStr, LockType.READ_LOCK, 0, Duration.ofSeconds(60)).get();

        // Intruder cannot add to tail
        assertLockDenied(client.addElementToTail(key, null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));

        // Intruder cannot add to head
        assertLockDenied(client.addElementToHead(key, null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));

        // Intruder cannot get and remove front
        assertLockDenied(client.getAndRemoveFront(key, null, 1, TIMEOUT));

        // Intruder cannot get and remove tail
        assertLockDenied(client.getAndRemoveTail(key, null, 1, TIMEOUT));

        // Intruder cannot remove head
        assertLockDenied(client.removeHead(key, null, 1, TIMEOUT));

        // Intruder cannot remove tail
        assertLockDenied(client.removeTail(key, null, 1, TIMEOUT));
    }

    // 15c. WRITE_LOCK behavior

    @Test
    @DisplayName("lock: WRITE_LOCK owner reads/writes, others blocked")
    void testWriteLockOwnerReadsWritesOthersBlocked() throws ExecutionException, InterruptedException {
        String key = "writeLockOwner" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 10;
        client.lockObject(key, LockType.WRITE_LOCK, clientId, Duration.ofSeconds(60)).get();

        // Owner can read
        Payload head = client.getHead(key,clientId).get();
        assertNotNull(head);

        // Owner can write
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))),clientId).get();
        assertEquals(1, added);

        // Intruder cannot read
        try {
            client.getHead(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT).get();
            Assertions.fail("Intruder should be blocked by WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot write
        assertLockDenied(client.addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("intruder".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));
    }

    @Test
    @DisplayName("lock: WRITE_LOCK all read ops blocked for others")
    void testWriteLockAllReadOpsBlockedForOthers() throws ExecutionException, InterruptedException {
        String key = "writeLockReadOps" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        client.lockObject(key, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        // Intruder cannot get front
        try {
            client.getFront(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT).get();
            Assertions.fail("Intruder should be blocked");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot get head
        try {
            client.getHead(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT).get();
            Assertions.fail("Intruder should be blocked");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot get tail
        try {
            client.getTail(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT).get();
            Assertions.fail("Intruder should be blocked");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot get and remove front
        assertLockDenied(client.getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT));

        // Intruder cannot get and remove tail
        assertLockDenied(client.getAndRemoveTail(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT));
    }

    @Test
    @DisplayName("lock: WRITE_LOCK all write ops blocked for others")
    void testWriteLockAllWriteOpsBlockedForOthers() throws ExecutionException, InterruptedException {
        String key = "writeLockWriteOps" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        client.lockObject(key, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        // Intruder cannot add to tail
        assertLockDenied(client.addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));

        // Intruder cannot add to head
        assertLockDenied(client.addElementToHead(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));

        // Intruder cannot get and remove front
        assertLockDenied(client.getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT));

        // Intruder cannot get and remove tail
        assertLockDenied(client.getAndRemoveTail(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT));

        // Intruder cannot remove head
        assertLockDenied(client.removeHead(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT));

        // Intruder cannot remove tail
        assertLockDenied(client.removeTail(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT));
    }

    // 15d. GLOBAL lock behavior

    @Test
    @DisplayName("lock: GLOBAL_LOCK owner works, others blocked")
    void testGlobalLockOwnerWorksOthersBlocked() throws ExecutionException, InterruptedException {
        String key = "globalLockOwner" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 2;
        client.lockObject(key, LockType.GLOBAL, clientId, Duration.ofSeconds(60)).get();

        // Owner can read
        Payload head = client.getHead(key,clientId).get();
        assertNotNull(head);

        // Owner can write
        Integer added = client.addElementToTail(key, null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))),clientId).get();
        assertEquals(1, added);

        // Intruder cannot read
        try {
            client.getHead(key.getBytes(StandardCharsets.UTF_8), null, 1, TIMEOUT).get();
            Assertions.fail("Intruder should be blocked by GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder cannot write
        assertLockDenied(client.addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("intruder".getBytes(StandardCharsets.UTF_8))), 1, TIMEOUT));
    }

    @Test
    @DisplayName("lock: GLOBAL_LOCK owner can remove queue")
    void testGlobalLockOwnerCanRemove() throws ExecutionException, InterruptedException {
        String key = "globalLockRemove" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 10;
        client.lockObject(key, LockType.GLOBAL, clientId, Duration.ofSeconds(60)).get();

        // Owner can remove
        boolean removed = client.remove(key,clientId).get();
        assertTrue(removed);

        // Unlock returns NOT_FOUND
        try {
            LockStatus unlockRes = client.unlockObject(key, clientId).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

    }

    @Test
    @DisplayName("lock: cannot lock by other client when already locked")
    void testCantLockByOtherClient() throws ExecutionException, InterruptedException {
        String key = "cantLockByOther" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        client.lockObject(key, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        assertLockDenied(client.lockObject(key, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)));

    }

    // =========================================================================
    // 16. UNLOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("unlock: owner can unlock WRITE_LOCK")
    void testUnlockByOwner() throws ExecutionException, InterruptedException {
        String key = "unlockByOwner" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 10;
        client.lockObject(key, LockType.WRITE_LOCK, clientId, Duration.ofSeconds(60)).get();

        LockStatus unlockRes = client.unlockObject(key, clientId).get();
        assertEquals(LockStatus.OK, unlockRes);
    }

    @Test
    @DisplayName("unlock: non-owner cannot unlock")
    void testCantUnlockByNonOwner() throws ExecutionException, InterruptedException {
        int ownerId = 100;
        int intruderId = 200;

        String key = "cantUnlockByNonOwner" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        client.lockObject(key, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();

        assertLockDenied(client.unlockObject(key, intruderId));


        LockStatus successUnlock = client.unlockObject(key, ownerId).get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("unlock: unlock after queue deleted returns NOT_FOUND")
    void testUnlockAfterQueueDeleted() throws ExecutionException, InterruptedException {
        String key = "unlockAfterDelete" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        int clientId = 7;
        client.lockObject(key, LockType.WRITE_LOCK, clientId, Duration.ofSeconds(60)).get();

        // Delete queue while locked
        boolean removed = client.remove(key,clientId).get();
        assertTrue(removed);

        // Unlock returns NOT_FOUND
        try {
            client.unlockObject(key, clientId).get();
            Assertions.fail("Expected NOT_FOUND error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 17. UNSUPPORTED METHODS ERROR CASES
    // =========================================================================

    @Test
    @DisplayName("unsupported: getElementAtPosition returns INTERNAL")
    void testGetElementAtPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.getElementAtPosition(key.getBytes(StandardCharsets.UTF_8), null, 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: getAndRemoveElementAtPosition returns INTERNAL")
    void testGetAndRemoveElementAtPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue2" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.getAndRemoveElementAtPosition(key.getBytes(StandardCharsets.UTF_8), null, 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: addElementToPosition returns INTERNAL")
    void testAddElementToPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue3" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.addElementToPosition(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: removeElementAtPosition returns INTERNAL")
    void testRemoveElementAtPositionOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue4" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.removeElementAtPosition(key.getBytes(StandardCharsets.UTF_8), null, 0, 0, 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: addElementToPositionBefore returns INTERNAL")
    void testAddElementToPositionBeforeOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue5" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.addElementToPositionBefore(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), Payload.of("pivot".getBytes(StandardCharsets.UTF_8)), 0, TIMEOUT));
    }

    @Test
    @DisplayName("unsupported: addElementToPositionAfter returns INTERNAL")
    void testAddElementToPositionAfterOnQueue() throws ExecutionException, InterruptedException {
        String key = "posOpsQueue6" + UUID.randomUUID();

        client.createQueue(key, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();

        Thread.sleep(500);

        assertUnsupportedMethod(client.addElementToPositionAfter(key.getBytes(StandardCharsets.UTF_8), null, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), Payload.of("pivot".getBytes(StandardCharsets.UTF_8)), 0, TIMEOUT));
    }

    // =========================================================================
    // 18. MISSING QUEUE ERROR CASES
    // =========================================================================

    @Test
    @DisplayName("missing queue: getAndRemoveFront returns NOT_FOUND")
    void testGetAndRemoveFrontOnMissingQueue() throws ExecutionException, InterruptedException {
        String key = "missingQueueGetAndRemove" + UUID.randomUUID();

        try {
            client.getAndRemoveFront(key).get();
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