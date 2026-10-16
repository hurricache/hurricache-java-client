package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OrderedMapOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + STREAM ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("Create empty ordered map")
    void testCreateEmptyOrderedMap() throws ExecutionException, InterruptedException {
        String key = "ordered_map_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedMap(key, new HashMap<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create ordered map with bytes values")
    void testCreateOrderedMapWithBytes() throws ExecutionException, InterruptedException {
        String key = "ordered_map_bytes_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));
        data.put(OrderedPayload.of("key3".getBytes(), 3L), v("value3"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with int values")
    void testCreateOrderedMapWithInt() throws ExecutionException, InterruptedException {
        String key = "ordered_map_int_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), vi(10));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vi(20));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), vi(30));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.values().iterator().next().longValue());
    }

    @Test
    @DisplayName("Create ordered map with double values")
    void testCreateOrderedMapWithDouble() throws ExecutionException, InterruptedException {
        String key = "ordered_map_double_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), vd(1.1));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vd(2.2));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), vd(3.3));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with mixed types")
    void testCreateOrderedMapMixedTypes() throws ExecutionException, InterruptedException {
        String key = "ordered_map_mixed_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("str".getBytes(), 1L), v("hello"));
        data.put(OrderedPayload.of("int".getBytes(), 2L), vi(42));
        data.put(OrderedPayload.of("dbl".getBytes(), 3L), vd(3.14));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 2. GET/UPDATE/DELETE VALUE IN CONTAINER
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey on ordered map")
    void testGetValueInContainer() throws ExecutionException, InterruptedException {
        String key = "ordered_map_get_value_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Variant value = client.getValueInContainerByKey(key, null, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        value = client.getValueInContainerByKey(key, null, "key2".getBytes()).get();
        assertNotNull(value);
        assertEquals(100, value.longValue());
    }

    @Test
    @DisplayName("existInContainerByKey on ordered map")
    void testExistInContainer() throws ExecutionException, InterruptedException {
        String key = "ordered_map_exist_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Boolean exists = client.existInContainerByKey(key, null, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.existInContainerByKey(key, null, "key_missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("updateContainerValue on ordered map")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String key = "ordered_map_update_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("old_value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Variant previous = client.updateContainerValue(key, null, "key1".getBytes(), v("new_value")).get();
        assertNotNull(previous);
        assertEquals("old_value", new String(previous.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant current = client.getValueInContainerByKey(key, null, "key1".getBytes()).get();
        assertEquals("new_value", new String(current.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue with type switch on ordered map")
    void testUpdateContainerValueWithTypeSwitch() throws ExecutionException, InterruptedException {
        String key = "ordered_map_update_type_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("text"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        // Update from bytes to int
        Variant previous = client.updateContainerValue(key, null, "key1".getBytes(), vi(42)).get();
        assertEquals("text", new String(previous.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant current = client.getValueInContainerByKey(key, null, "key1".getBytes()).get();
        assertEquals(42, current.longValue());

        // Update from int to double
        previous = client.updateContainerValue(key, null, "key1".getBytes(), vd(3.14)).get();
        assertEquals(42, previous.longValue());

        current = client.getValueInContainerByKey(key, null, "key1".getBytes()).get();
        assertEquals(3.14, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("getAndDeleteValueInContainer on ordered map")
    void testGetAndDeleteValueInContainer() throws ExecutionException, InterruptedException {
        String key = "ordered_map_get_delete_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Variant deleted = client.getAndDeleteValueInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(deleted);
        assertEquals("value1", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Boolean exists = client.existInContainerByKey(key, null, "key1".getBytes()).get();
        assertFalse(exists);

        exists = client.existInContainerByKey(key, hint, "key2".getBytes()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("removeInContainerByKey on ordered map")
    void testRemoveInContainerByKey() throws ExecutionException, InterruptedException {
        String key = "ordered_map_remove_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Integer removed = client.removeInContainerByKey(key, null, "key1".getBytes()).get();
        assertEquals(1, removed);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 3. ADD ELEMENT TO ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("addElementOrderedMap")
    void testAddElementOrderedMap() throws ExecutionException, InterruptedException {
        String key = "ordered_map_add_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Integer added = client.addElementOrderedMap(key, hint,
                                                    List.of(OrderedPayload.of("key2".getBytes(), 2L), OrderedPayload.of("key3".getBytes(), 3L)),
                                                    List.of(vi(100), vd(3.14))).get();
        assertEquals(2, added);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 4. RANGE QUERIES
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap")
    void testStreamElementInRange() throws ExecutionException, InterruptedException {
        String key = "ordered_map_range_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("1".getBytes(), 1L), v("one"));
        data.put(OrderedPayload.of("2".getBytes(), 2L), v("two"));
        data.put(OrderedPayload.of("3".getBytes(), 3L), v("three"));
        data.put(OrderedPayload.of("4".getBytes(), 4L), v("four"));
        data.put(OrderedPayload.of("5".getBytes(), 5L), v("five"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Map<OrderedPayload, Variant> range = client.streamElementInRangeOrderedMap(key, null, 2L, 4L, false).get();
        assertEquals(3, range.size());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap with reverse")
    void testStreamElementInRangeReverse() throws ExecutionException, InterruptedException {
        String key = "ordered_map_range_reverse_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("1".getBytes(), 1L), v("one"));
        data.put(OrderedPayload.of("2".getBytes(), 2L), v("two"));
        data.put(OrderedPayload.of("3".getBytes(), 3L), v("three"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Map<OrderedPayload, Variant> range = client.streamElementInRangeOrderedMap(key, null, 1L, 3L, true).get();
        assertEquals(3, range.size());
    }

    // =========================================================================
    // 5. DELETE FROM ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("deleteFromContainerByKeyValue on ordered map")
    void testDeleteFromContainerByKeyValue() throws ExecutionException, InterruptedException {
        String key = "ordered_map_delete_value_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), v("val"));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), v("val"));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), v("other"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        // Delete by both key and value
        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.ORDERED_MAP,
                List.of(OrderedPayload.of("k1".getBytes(), 1L), OrderedPayload.of("k2".getBytes(), 2L)),
                List.of(v("val"), v("val"))).get();
        assertEquals(2, deleted);

        Map<OrderedPayload, Variant> stream = client.streamOrderedMap(key).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 6. SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize on ordered map")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "ordered_map_size_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), v("v1"));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), v("v2"));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), v("v3"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 7. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on ordered map: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "ordered_map_read_lock_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Map<OrderedPayload, Variant> val = client.streamOrderedMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Map<OrderedPayload, Variant> intruderVal = client.streamOrderedMap(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on ordered map: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "ordered_map_write_lock_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Map<OrderedPayload, Variant> val = client.streamOrderedMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        client.addElementOrderedMap(key, hint,
                List.of(OrderedPayload.of("key2".getBytes(), 2L)),
                List.of(v("added")),
                OWNER_CLIENT_ID).get();

        assertDenied(client.streamOrderedMap(key, null, INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on ordered map: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "ordered_map_global_lock_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Map<OrderedPayload, Variant> val = client.streamOrderedMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.streamOrderedMap(key, null, INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Lock expiration on ordered map")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "ordered_map_lock_expire_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        Map<OrderedPayload, Variant> val = client.streamOrderedMap(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}