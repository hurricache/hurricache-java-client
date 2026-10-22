package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueueOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE QUEUE
    // =========================================================================

    @Test
    @DisplayName("Create empty queue")
    void testCreateEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "queue_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, new ArrayList<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create queue with bytes")
    void testCreateQueueWithBytes() throws ExecutionException, InterruptedException {
        String key = "queue_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("item1", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create queue with int")
    void testCreateQueueWithInt() throws ExecutionException, InterruptedException {
        String key = "queue_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals(10, head.longValue());
    }

    @Test
    @DisplayName("Create queue with double")
    void testCreateQueueWithDouble() throws ExecutionException, InterruptedException {
        String key = "queue_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals(1.1, head.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Create queue with mixed types")
    void testCreateQueueMixedTypes() throws ExecutionException, InterruptedException {
        String key = "queue_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("hello", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 2. ADD/REMOVE
    // =========================================================================

    @Test
    @DisplayName("addElementToTail and addElementToHead")
    void testAddToTailAndHead() throws ExecutionException, InterruptedException {
        String key = "queue_add_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("middle"))).get();

        Integer added = client.pushTail(key, List.of(v("tail1"), v("tail2"))).get();
        assertEquals(2, added);

        added = client.pushHead(key, List.of(v("head1"))).get();
        assertEquals(1, added);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("head1", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant tail = client.peekTail(key).get();
        assertNotNull(tail);
        assertEquals("tail2", new String(tail.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail")
    void testGetHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "queue_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("first", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant tail = client.peekTail(key).get();
        assertNotNull(tail);
        assertEquals("last", new String(tail.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteHead")
    void testGetAndDeleteHead() throws ExecutionException, InterruptedException {
        String key = "queue_delete_head_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();

        Variant deleted = client.popHead(key).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant newHead = client.peekHead(key).get();
        assertEquals("middle", new String(newHead.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteHead and deleteTail")
    void testDeleteHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "queue_delete_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();

        Integer deleted = client.deleteHead(key).get();
        assertEquals(1, deleted);

        deleted = client.deleteTail(key).get();
        assertEquals(1, deleted);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("middle", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteTail")
    void testGetAndDeleteTail() throws ExecutionException, InterruptedException {
        String key = "queue_get_delete_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();

        Variant deleted = client.popTail(key).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant tail = client.peekTail(key).get();
        assertNotNull(tail);
        assertEquals("middle", new String(tail.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToHead")
    void testAddElementToHead() throws ExecutionException, InterruptedException {
        String key = "queue_add_head_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("middle"))).get();

        Integer added = client.pushHead(key, List.of(v("head1"), v("head2"))).get();
        assertEquals(2, added);

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("head2", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant tail = client.peekTail(key).get();
        assertNotNull(tail);
        assertEquals("middle", new String(tail.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize via getAndDeleteHead")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "queue_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createQueue(key, data).get();

        // Count elements by getting and deleting head
        int count = 0;
        while (true) {
            Variant head = client.popHead(key).get();
            if (head == null || head.isNone()) break;
            count++;
        }
        assertEquals(3, count);
    }

    // =========================================================================
    // 4. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on queue: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "queue_read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.peekHead(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Variant intruderVal = client.peekHead(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.pushTail(key, null, List.of(v("added")), OWNER_CLIENT_ID));
        assertDenied(client.pushTail(key, null, List.of(v("added")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on queue: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "queue_write_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Variant val = client.peekHead(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        client.pushTail(key, null, List.of(v("added")), OWNER_CLIENT_ID).get();

        assertDenied(client.peekHead(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on queue: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "queue_global_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Variant val = client.peekHead(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.peekHead(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Lock expiration on queue")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "queue_lock_expire_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        Variant val = client.peekHead(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}