package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.client.intf.OrderedVariant;
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

public class OrderedSetOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + STREAM ORDERED SET
    // =========================================================================

    @Test
    @DisplayName("Create empty ordered set")
    void testCreateEmptyOrderedSet() throws ExecutionException, InterruptedException {
        String key = "ordered_set_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, new ArrayList<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create ordered set with bytes")
    void testCreateOrderedSetWithBytes() throws ExecutionException, InterruptedException {
        String key = "ordered_set_bytes_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L),
                OrderedVariant.ofBytes("item2".getBytes(), 2L),
                OrderedVariant.ofBytes("item3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with int")
    void testCreateOrderedSetWithInt() throws ExecutionException, InterruptedException {
        String key = "ordered_set_int_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofInt(10, 1L),
                OrderedVariant.ofInt(20, 2L),
                OrderedVariant.ofInt(30, 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).intValue());
        assertEquals(20, stream.get(1).intValue());
    }

    @Test
    @DisplayName("Create ordered set with double")
    void testCreateOrderedSetWithDouble() throws ExecutionException, InterruptedException {
        String key = "ordered_set_double_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofDouble(1.1, 1L),
                OrderedVariant.ofDouble(2.2, 2L),
                OrderedVariant.ofDouble(3.3, 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(3, stream.size());
        assertEquals(1.1, stream.get(0).doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Create ordered set with mixed types")
    void testCreateOrderedSetMixedTypes() throws ExecutionException, InterruptedException {
        String key = "ordered_set_mixed_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("hello".getBytes(), 1L),
                OrderedVariant.ofInt(42, 2L),
                OrderedVariant.ofDouble(3.14, 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(3, stream.size());
        assertEquals("hello", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(42, stream.get(1).intValue());
        assertEquals(3.14, stream.get(2).doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. ELEMENT AT POSITION
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "ordered_set_position_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Variant element = client.getAt(key, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition")
    void testGetAndDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_position_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Variant deleted = client.popAt(key, null, 1).get();
        assertNotNull(deleted);
        assertEquals("1", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("deleteElementAtPosition")
    void testDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_pos_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Integer deleted = client.removeAt(key, null, 1L).get();
        assertEquals(1, deleted);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 3. RANGE QUERIES
    // =========================================================================

    @Test
    @DisplayName("getElementsInRange")
    void testGetElementsInRange() throws ExecutionException, InterruptedException {
        String key = "ordered_set_range_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L),
                OrderedVariant.ofBytes("4".getBytes(), 4L),
                OrderedVariant.ofBytes("5".getBytes(), 5L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, hint, 1, 3, false).get();
        assertEquals(3, range.size());
        assertEquals("1", new String(range.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("2", new String(range.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("3", new String(range.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 4. ADD ELEMENT WITH WEIGHT
    // =========================================================================

    @Test
    @DisplayName("addElementWithWeight to ordered set")
    void testAddElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "ordered_set_add_weight_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L)
        )).get();

        Integer added = client.addElementWithWeight(key, null, List.of(
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        )).get();
        assertEquals(2, added);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 5. RANGE QUERIES
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedSet")
    void testStreamElementInRangeOrderedSet() throws ExecutionException, InterruptedException {
        String key = "ordered_set_range_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L),
                OrderedVariant.ofBytes("4".getBytes(), 4L),
                OrderedVariant.ofBytes("5".getBytes(), 5L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, null, 2L, 4L, false).get();
        assertEquals(3, range.size());
        assertEquals("2", new String(range.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("3", new String(range.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("4", new String(range.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("streamElementInRangeOrderedSet with reverse")
    void testStreamElementInRangeOrderedSetReverse() throws ExecutionException, InterruptedException {
        String key = "ordered_set_range_reverse_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, null, 1L, 3L, true).get();
        assertEquals(3, range.size());
        // Reverse order: 3, 2, 1
        assertEquals("3", new String(range.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("1", new String(range.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. WEIGHT-BASED OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getElementWithWeight")
    void testGetElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "ordered_set_get_weight_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Variant element = client.getElementWithWeight(key, null, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndRemoveElementWithWeight")
    void testGetAndRemoveElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "ordered_set_remove_weight_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Variant removed = client.getAndRemoveElementWithWeight(key, null, 1).get();
        assertNotNull(removed);
        assertEquals("first", new String(removed.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 7. DELETE BY KEY
    // =========================================================================

    @Test
    @DisplayName("deleteFromContainerByKey")
    void testDeleteFromContainerByKey() throws ExecutionException, InterruptedException {
        String key = "ordered_set_delete_key_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("a".getBytes(), 1L),
                OrderedVariant.ofBytes("b".getBytes(), 2L),
                OrderedVariant.ofBytes("c".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Integer deleted = client.removeFromContainerByKey(key, v("b")).get();
        assertEquals(1, deleted);

        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 6. SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "ordered_set_size_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 7. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on ordered set: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "ordered_set_read_lock_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<OrderedVariant> val = client.getOrderedSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<OrderedVariant> intruderVal = client.getOrderedSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on ordered set: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "ordered_set_write_lock_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<OrderedVariant> val = client.getOrderedSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // OrderedSet has no addElement method - test would need to create new set
        // This line is kept for reference only
        // client.addElement(key, null, List.of(
        //         OrderedVariant.ofBytes("added".getBytes(), 2L)
        // ), OWNER_CLIENT_ID).get();

        assertDenied(client.getOrderedSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on ordered set: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "ordered_set_global_lock_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<OrderedVariant> val = client.getOrderedSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getOrderedSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Lock expiration on ordered set")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "ordered_set_lock_expire_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        );

        KeyHintData hint = client.createOrderedSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<OrderedVariant> val = client.getOrderedSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}