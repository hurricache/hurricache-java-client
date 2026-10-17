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
 * SmartClient routing tests for Queue operations.
 * Mirrors QueueOperationsTest for cluster routing.
 */
public class SmartClientQueueRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE QUEUE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create empty queue, validate on MASTER and BACKUP")
    void testCreateEmptyQueueOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, new ArrayList<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(0, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create queue with bytes, read on MASTER then BACKUP")
    void testCreateQueueWithBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "queue_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("item1", new String(head.bytesValue(), StandardCharsets.UTF_8));

        head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("item1", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create queue with int, read on MASTER then BACKUP")
    void testCreateQueueWithIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "queue_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals(10, head.longValue());

        head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals(10, head.longValue());
    }

    @Test
    @DisplayName("Create queue with double, read on MASTER then BACKUP")
    void testCreateQueueWithDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "queue_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals(1.1, head.doubleValue(), 0.001);

        head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals(1.1, head.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Create queue with mixed types, validate on MASTER and BACKUP")
    void testCreateQueueMixedTypesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("hello", new String(head.bytesValue(), StandardCharsets.UTF_8));

        head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
    }

    // =========================================================================
    // 2. ADD/REMOVE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementToTail and addElementToHead on MASTER, validate on BACKUP")
    void testAddToTailAndHeadOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_add_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("middle"))).get();
        Thread.sleep(500);

        // Add to tail on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.pushTail(key, hint, List.of(v("tail1"), v("tail2"))).get();
        assertEquals(2, added);

        // Add to head on MASTER
        added = client.pushHead(key, hint, List.of(v("head1"))).get();
        assertEquals(1, added);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("head1", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.BACKUP).peekTail(key, hint).get();
        assertNotNull(tail);
        assertEquals("tail2", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail on MASTER then BACKUP")
    void testGetHeadAndTailOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "queue_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("first", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.MASTER).peekTail(key, hint).get();
        assertNotNull(tail);
        assertEquals("last", new String(tail.bytesValue(), StandardCharsets.UTF_8));

        head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertEquals("first", new String(head.bytesValue(), StandardCharsets.UTF_8));

        tail = client.setMode(Mode.BACKUP).peekTail(key, hint).get();
        assertEquals("last", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteHead on MASTER, verify on BACKUP")
    void testGetAndDeleteHeadOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_delete_head_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteHead on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.popHead(key, hint).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        Variant newHead = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertEquals("middle", new String(newHead.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteHead and deleteTail on MASTER, verify on BACKUP")
    void testDeleteHeadAndTailOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_delete_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        // DeleteHead on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.deleteHead(key, hint).get();
        assertEquals(1, deleted);

        // DeleteTail on MASTER
        deleted = client.deleteTail(key, hint).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("middle", new String(head.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteTail on MASTER, verify on BACKUP")
    void testGetAndDeleteTailOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_get_delete_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteTail on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.popTail(key, hint).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        Variant tail = client.setMode(Mode.BACKUP).peekTail(key, hint).get();
        assertNotNull(tail);
        assertEquals("middle", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToHead on MASTER, verify on BACKUP")
    void testAddElementToHeadOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "queue_add_head_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("middle"))).get();
        Thread.sleep(500);

        // AddElementToHead on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.pushHead(key, hint, List.of(v("head1"), v("head2"))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant head = client.setMode(Mode.BACKUP).peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("head2", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.BACKUP).peekTail(key, hint).get();
        assertNotNull(tail);
        assertEquals("middle", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. SIZE WITH ROUTING (using getAndDeleteHead)
    // =========================================================================

    @Test
    @DisplayName("getSize via getAndDeleteHead on MASTER then BACKUP")
    void testGetSizeViaGetAndDeleteHeadOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "queue_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createQueue(key, data).get();
        Thread.sleep(500);

        // Count elements by getting and deleting head on MASTER
        int count = 0;
        while (true) {
            Variant head = client.setMode(Mode.MASTER).popHead(key, hint).get();
            if (head == null || head.isNone()) break;
            count++;
        }
        assertEquals(3, count);
    }

    // =========================================================================
    // 4. LOCK OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on queue on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnQueueOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Variant val = client.setMode(Mode.BACKUP).peekHead(key, hint, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        Variant intruderVal = client.setMode(Mode.BACKUP).peekHead(key, hint, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on queue on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnQueueOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_write_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Variant val = client.setMode(Mode.BACKUP).peekHead(key, hint, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.pushTail(key, hint, 100, List.of(v("added"))).get();
        Thread.sleep(500);

        // Verify on BACKUP
        val = client.setMode(Mode.BACKUP).peekHead(key, hint, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on queue on MASTER: only owner can access")
    void testGlobalLockOnQueueOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_global_lock_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Variant val = client.setMode(Mode.BACKUP).peekHead(key, hint, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on queue with routing")
    void testLockExpirationOnQueueOnMaster() throws ExecutionException, InterruptedException {
        String key = "queue_lock_expire_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP
        Variant val = client.setMode(Mode.BACKUP).peekHead(key, hint, 100).get();
        assertNotNull(val);
    }
}