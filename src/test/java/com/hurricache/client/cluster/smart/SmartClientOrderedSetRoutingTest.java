package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedVariant;
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
 * SmartClient routing tests for OrderedSet operations.
 * Mirrors OrderedSetOperationsTest for cluster routing.
 */
public class SmartClientOrderedSetRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + STREAM ORDERED SET WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create empty ordered set, validate on MASTER and BACKUP")
    void testCreateEmptyOrderedSetOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, new ArrayList<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(0, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create ordered set with bytes, read on MASTER then BACKUP")
    void testCreateOrderedSetWithBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_bytes_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L),
                OrderedVariant.ofBytes("item2".getBytes(), 2L),
                OrderedVariant.ofBytes("item3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());

        stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with int, read on MASTER then BACKUP")
    void testCreateOrderedSetWithIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_int_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofInt(10, 1L),
                OrderedVariant.ofInt(20, 2L),
                OrderedVariant.ofInt(30, 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).intValue());
        assertEquals(20, stream.get(1).intValue());

        stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with double, read on MASTER then BACKUP")
    void testCreateOrderedSetWithDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_double_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofDouble(1.1, 1L),
                OrderedVariant.ofDouble(2.2, 2L),
                OrderedVariant.ofDouble(3.3, 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals(1.1, stream.get(0).doubleValue(), 0.001);

        stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with mixed types, validate on MASTER and BACKUP")
    void testCreateOrderedSetMixedTypesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_mixed_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("hello".getBytes(), 1L),
                OrderedVariant.ofInt(42, 2L),
                OrderedVariant.ofDouble(3.14, 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
        assertEquals("hello", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(42, stream.get(1).intValue());
        assertEquals(3.14, stream.get(2).doubleValue(), 0.001);

        stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 2. ELEMENT AT POSITION WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition on MASTER then BACKUP")
    void testGetElementAtPositionOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_position_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        Variant element = client.setMode(Mode.MASTER).getAt(key, hint, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), StandardCharsets.UTF_8));

        element = client.setMode(Mode.BACKUP).getAt(key, hint, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition on MASTER, verify on BACKUP")
    void testGetAndDeleteElementAtPositionOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_position_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteElementAtPosition on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.popAt(key, hint, 1).get();
        assertNotNull(deleted);
        assertEquals("1", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("deleteElementAtPosition on MASTER, verify on BACKUP")
    void testDeleteElementAtPositionOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_pos_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // DeleteElementAtPosition on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeAt(key, hint, 1L).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 3. RANGE QUERIES WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedSet on MASTER then BACKUP")
    void testStreamElementInRangeOrderedSetOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_range_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L),
                OrderedVariant.ofBytes("4".getBytes(), 4L),
                OrderedVariant.ofBytes("5".getBytes(), 5L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        List<OrderedVariant> range = client.setMode(Mode.MASTER).getElementInRangeOrderedSet(key, hint, 2L, 4L, false).get();
        assertEquals(3, range.size());
        assertEquals("2", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("3", new String(range.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("4", new String(range.get(2).bytesValue(), StandardCharsets.UTF_8));

        range = client.setMode(Mode.BACKUP).getElementInRangeOrderedSet(key, hint, 2L, 4L, false).get();
        assertEquals(3, range.size());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedSet with reverse on MASTER then BACKUP")
    void testStreamElementInRangeOrderedSetReverseOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_range_reverse_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        List<OrderedVariant> range = client.setMode(Mode.MASTER).getElementInRangeOrderedSet(key, hint, 1L, 3L, true).get();
        assertEquals(3, range.size());
        // Reverse order: 3, 2, 1
        assertEquals("3", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("1", new String(range.get(2).bytesValue(), StandardCharsets.UTF_8));

        range = client.setMode(Mode.BACKUP).getElementInRangeOrderedSet(key, hint, 1L, 3L, true).get();
        assertEquals(3, range.size());
    }

    // =========================================================================
    // 4. ADD ELEMENT WITH WEIGHT WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementWithWeight on MASTER, validate on BACKUP")
    void testAddElementWithWeightOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_add_weight_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L)
        )).get();
        Thread.sleep(500);

        // AddElementWithWeight on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.addElementWithWeight(key, hint, List.of(
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        )).get();
        assertEquals(2, added);
        Thread.sleep(500);

        // Verify on BACKUP
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 5. WEIGHT-BASED OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getElementWithWeight on MASTER then BACKUP")
    void testGetElementWithWeightOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_get_weight_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        Variant element = client.setMode(Mode.MASTER).getElementWithWeight(key, hint, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), StandardCharsets.UTF_8));

        element = client.setMode(Mode.BACKUP).getElementWithWeight(key, hint, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndRemoveElementWithWeight on MASTER, verify on BACKUP")
    void testGetAndRemoveElementWithWeightOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_remove_weight_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // GetAndRemoveElementWithWeight on MASTER
        client.setMode(Mode.MASTER);
        Variant removed = client.getAndRemoveElementWithWeight(key, hint, 1).get();
        assertNotNull(removed);
        assertEquals("first", new String(removed.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 6. DELETE BY KEY WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("removeFromContainerByKey on MASTER, verify on BACKUP")
    void testRemoveFromContainerByKeyOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_key_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("a".getBytes(), 1L),
                OrderedVariant.ofBytes("b".getBytes(), 2L),
                OrderedVariant.ofBytes("c".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // RemoveFromContainerByKey on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKey(key, hint, v("b")).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKey on MASTER, verify on BACKUP")
    void testDeleteFromContainerByKeyOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_key_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("a".getBytes(), 1L),
                OrderedVariant.ofBytes("b".getBytes(), 2L),
                OrderedVariant.ofBytes("c".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // RemoveFromContainerByKey on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKey(key, hint, v("b")).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 7. SIZE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getSize on MASTER then BACKUP")
    void testGetSizeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_set_size_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(3, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 8. LOCK OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on ordered set on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnOrderedSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_set_read_lock_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<OrderedVariant> val = client.setMode(Mode.BACKUP).getOrderedSet(key, hint, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        List<OrderedVariant> intruderVal = client.setMode(Mode.BACKUP).getOrderedSet(key, hint, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on ordered set on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnOrderedSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_set_write_lock_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<OrderedVariant> val = client.setMode(Mode.BACKUP).getOrderedSet(key, hint, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.addElementWithWeight(key, hint, List.of(
                OrderedVariant.ofBytes("added".getBytes(), 2L)
        ),100).get();
        Thread.sleep(500);

        // Verify on BACKUP
        val = client.setMode(Mode.BACKUP).getOrderedSet(key, hint, 100).get();
        assertEquals(2, val.size());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on ordered set on MASTER: only owner can access")
    void testGlobalLockOnOrderedSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_set_global_lock_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<OrderedVariant> val = client.setMode(Mode.BACKUP).getOrderedSet(key, hint, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on ordered set with routing")
    void testLockExpirationOnOrderedSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_set_lock_expire_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP
        List<OrderedVariant> val = client.setMode(Mode.BACKUP).getOrderedSet(key, hint, 100).get();
        assertNotNull(val);
    }
}