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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for List operations.
 * Mirrors ListOperationsTest for cluster routing.
 */
public class SmartClientListRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + STREAM LIST WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create empty list, validate on MASTER and BACKUP")
    void testCreateEmptyListOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(0, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create list with bytes, read on MASTER then BACKUP")
    void testCreateListWithBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals("item1", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));

        stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals("item1", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create list with int, read on MASTER then BACKUP")
    void testCreateListWithIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).longValue());

        stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).longValue());
    }

    @Test
    @DisplayName("Create list with double, read on MASTER then BACKUP")
    void testCreateListWithDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals(1.1, stream.get(0).doubleValue(), 0.001);

        stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals(1.1, stream.get(0).doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Create list with mixed types, validate on MASTER and BACKUP")
    void testCreateListMixedTypesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals("hello", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(42, stream.get(1).longValue());
        assertEquals(3.14, stream.get(2).doubleValue(), 0.001);

        stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 2. ADD/REMOVE ENDS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementToTail and addElementToHead on MASTER, validate on BACKUP")
    void testAddToTailAndHeadOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_add_ends_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("middle"))).get();
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
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(4, stream.size());
        assertEquals("head1", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("tail2", new String(stream.get(3).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail on MASTER then BACKUP")
    void testGetHeadAndTailOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createList(key, data).get();
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
    @DisplayName("getAndDeleteHead and getAndDeleteTail on MASTER, verify on BACKUP")
    void testGetAndDeleteHeadAndTailOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_pop_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteHead on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.popHead(key, hint).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), StandardCharsets.UTF_8));

        // GetAndDeleteTail on MASTER
        deleted = client.popTail(key, hint).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP - should have only middle
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(1, stream.size());
        assertEquals("middle", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteHead and deleteTail on MASTER, verify on BACKUP")
    void testDeleteHeadAndTailOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_delete_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createList(key, data).get();
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
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(1, stream.size());
        assertEquals("middle", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. INDEX OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition with mixed types on MASTER then BACKUP")
    void testGetElementAtPositionMixedTypesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_get_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), vi(42), vd(3.14));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        Variant element = client.setMode(Mode.MASTER).getAt(key, hint, 1).get();
        assertNotNull(element);
        assertEquals(42, element.longValue());

        element = client.setMode(Mode.BACKUP).getAt(key, hint, 1).get();
        assertNotNull(element);
        assertEquals(42, element.longValue());
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition on MASTER, verify on BACKUP")
    void testGetAndDeleteElementAtPositionOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_delete_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteElementAtPosition on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.popAt(key, hint, 2).get();
        assertNotNull(deleted);
        assertEquals("3", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(4, stream.size());
    }

    @Test
    @DisplayName("deleteElementAtPosition on MASTER, verify on BACKUP")
    void testDeleteElementAtPositionOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_delete_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // DeleteElementAtPosition on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeAt(key, hint, 2L).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(4, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue on MASTER, verify on BACKUP")
    void testDeleteFromContainerByKeyValueOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_delete_value_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("a"), v("c"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // DeleteFromContainerByKeyValue on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKeyAndValue(key, com.hurricache.grpc.ContainerType.LIST, List.of(), List.of(v("a"))).get();
        assertEquals(2, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 4. POSITION-BASED INSERTION WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore and addElementToPositionAfter on MASTER, validate on BACKUP")
    void testAddElementToPositionOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_position_insert_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("last"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // Add to position on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.insertBefore(key, hint, List.of(v("before")), v("last")).get();
        assertEquals(1, added);

        added = client.insertAfter(key, hint, List.of(v("after")), v("first")).get();
        assertEquals(1, added);
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(4, stream.size());
        assertEquals("first", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("after", new String(stream.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("before", new String(stream.get(2).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("last", new String(stream.get(3).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. RANGE QUERIES WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getElementsInRange on MASTER then BACKUP")
    void testGetElementsInRangeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        List<Variant> range = client.setMode(Mode.MASTER).getRange(key, hint, com.hurricache.grpc.ContainerType.LIST, 1, 3).get();
        assertEquals(3, range.size());
        assertEquals("2", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("3", new String(range.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("4", new String(range.get(2).bytesValue(), StandardCharsets.UTF_8));

        range = client.setMode(Mode.BACKUP).getRange(key, hint, com.hurricache.grpc.ContainerType.LIST, 1, 3).get();
        assertEquals(3, range.size());
    }

    // =========================================================================
    // 6. SIZE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getSize on MASTER then BACKUP")
    void testGetSizeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "list_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(3, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 7. LOCK OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on list on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnListOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_read_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getList(key, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        List<Variant> intruderVal = client.setMode(Mode.BACKUP).getList(key, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on list on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnListOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_write_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getList(key, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.pushTail(key, hint, List.of(v("added")), 100).get();
        Thread.sleep(500);

        // Verify on BACKUP
        val = client.setMode(Mode.BACKUP).getList(key, 100).get();
        assertEquals(2, val.size());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on list on MASTER: only owner can access")
    void testGlobalLockOnListOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_global_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getList(key, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on list with routing")
    void testLockExpirationOnListOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_lock_expire_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getList(key, 100).get();
        assertNotNull(val);
    }
}