package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Queue operations WITHOUT keyHint.
 */
public class SmartClientQueueNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("Create empty queue, read on MASTER without keyHint")
    void testCreateEmptyQueueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, new ArrayList<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create queue with bytes, read on MASTER without keyHint")
    void testCreateQueueWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key).get();
        assertNotNull(head);
        assertEquals("item1", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create queue with int, read on MASTER without keyHint")
    void testCreateQueueWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key).get();
        assertNotNull(head);
        assertEquals(10, head.longValue());
    }

    @Test
    @DisplayName("Create queue with mixed types, read on MASTER without keyHint")
    void testCreateQueueMixedTypesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key).get();
        assertNotNull(head);
        assertEquals("hello", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToTail and addElementToHead on MASTER without keyHint")
    void testAddToTailAndHeadWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_add_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("middle"))).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer added = client.pushTail(key, List.of(v("tail1"), v("tail2"))).get();
        assertEquals(2, added);

        added = client.pushHead(key, List.of(v("head1"))).get();
        assertEquals(1, added);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.BACKUP).peekHead(key).get();
        assertNotNull(head);
        assertEquals("head1", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.BACKUP).peekTail(key).get();
        assertNotNull(tail);
        assertEquals("tail2", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail on MASTER without keyHint")
    void testGetHeadAndTailWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key).get();
        assertNotNull(head);
        assertEquals("first", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.MASTER).peekTail(key).get();
        assertNotNull(tail);
        assertEquals("last", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteHead on MASTER without keyHint")
    void testGetAndDeleteHeadWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_delete_head_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Variant deleted = client.popHead(key).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        Variant newHead = client.setMode(Mode.BACKUP).peekHead(key).get();
        assertEquals("middle", new String(newHead.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteHead and deleteTail on MASTER without keyHint")
    void testDeleteHeadAndTailWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_delete_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer deleted = client.deleteHead(key).get();
        assertEquals(1, deleted);

        deleted = client.deleteTail(key).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.BACKUP).peekHead(key).get();
        assertNotNull(head);
        assertEquals("middle", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteTail on MASTER without keyHint")
    void testGetAndDeleteTailWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_get_delete_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Variant deleted = client.popTail(key).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        Variant tail = client.setMode(Mode.BACKUP).peekTail(key).get();
        assertNotNull(tail);
        assertEquals("middle", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToHead on MASTER without keyHint")
    void testAddElementToHeadWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_add_head_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("middle"))).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer added = client.pushHead(key, List.of(v("head1"), v("head2"))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.BACKUP).peekHead(key).get();
        assertNotNull(head);
        assertEquals("head2", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.BACKUP).peekTail(key).get();
        assertNotNull(tail);
        assertEquals("middle", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getSize via getAndDeleteHead on MASTER without keyHint")
    void testGetSizeViaGetAndDeleteHeadWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        int count = 0;
        while (true) {
            Variant head = client.setMode(Mode.MASTER).popHead(key).get();
            if (head == null || head.isNone()) break;
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    @DisplayName("READ_LOCK on queue on MASTER without keyHint")
    void testReadLockOnQueueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, null, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Variant val = client.setMode(Mode.BACKUP).peekHead(key, 100).get();
        assertNotNull(val);

        Variant intruderVal = client.setMode(Mode.BACKUP).peekHead(key, 999).get();
        assertNotNull(intruderVal);

        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on queue on MASTER without keyHint")
    void testWriteLockOnQueueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_write_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, null, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Variant val = client.setMode(Mode.BACKUP).peekHead(key, 100).get();
        assertNotNull(val);

        client.pushTail(key, 100, List.of(v("added"))).get();
        Thread.sleep(500);

        val = client.setMode(Mode.BACKUP).peekHead(key, 100).get();
        assertNotNull(val);

        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on queue on MASTER without keyHint")
    void testGlobalLockOnQueueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_global_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, null, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Variant val = client.setMode(Mode.BACKUP).peekHead(key, 100).get();
        assertNotNull(val);

        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on queue without keyHint")
    void testLockExpirationOnQueueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_no_hint_lock_expire_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, null, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        Variant val = client.setMode(Mode.BACKUP).peekHead(key, 100).get();
        assertNotNull(val);
    }
}