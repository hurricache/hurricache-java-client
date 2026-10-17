package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for OrderedMap operations.
 * Mirrors OrderedMapOperationsTest for cluster routing.
 */
public class SmartClientOrderedMapRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + STREAM ORDERED MAP WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create empty ordered map, validate on MASTER and BACKUP")
    void testCreateEmptyOrderedMapOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedMap(key, new HashMap<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(0, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create ordered map with bytes values, read on MASTER then BACKUP")
    void testCreateOrderedMapWithBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_bytes_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));
        data.put(OrderedPayload.of("key3".getBytes(), 3L), v("value3"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());

        stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with int values, read on MASTER then BACKUP")
    void testCreateOrderedMapWithIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_int_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), vi(10));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vi(20));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), vi(30));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());

        stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with double values, read on MASTER then BACKUP")
    void testCreateOrderedMapWithDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_double_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), vd(1.1));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vd(2.2));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), vd(3.3));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());

        stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with mixed types, validate on MASTER and BACKUP")
    void testCreateOrderedMapMixedTypesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_mixed_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("str".getBytes(), 1L), v("hello"));
        data.put(OrderedPayload.of("int".getBytes(), 2L), vi(42));
        data.put(OrderedPayload.of("dbl".getBytes(), 3L), vd(3.14));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());

        stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 2. GET/UPDATE/DELETE VALUE IN CONTAINER WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey on ordered map on MASTER then BACKUP")
    void testGetValueInContainerByKeyOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_get_value_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        Variant value = client.setMode(Mode.MASTER).getInContainer(key, hint, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), StandardCharsets.UTF_8));

        value = client.setMode(Mode.BACKUP).getInContainer(key, hint, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), StandardCharsets.UTF_8));

        value = client.setMode(Mode.MASTER).getInContainer(key, hint, "key2".getBytes()).get();
        assertEquals(100, value.longValue());
    }

    @Test
    @DisplayName("existInContainerByKey on ordered map on MASTER then BACKUP")
    void testExistInContainerByKeyOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_exist_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, hint, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.setMode(Mode.BACKUP).containsKey(key, hint, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.setMode(Mode.MASTER).containsKey(key, hint, "key_missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("updateContainerValue on ordered map on MASTER, validate on BACKUP")
    void testUpdateContainerValueOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_update_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("old_value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // Update on MASTER
        client.setMode(Mode.MASTER);
        Variant previous = client.update(key, hint, "key1".getBytes(), v("new_value")).get();
        assertNotNull(previous);
        assertEquals("old_value", new String(previous.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, hint, "key1".getBytes()).get();
        assertEquals("new_value", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue with type switch on ordered map on MASTER, validate on BACKUP")
    void testUpdateContainerValueWithTypeSwitchOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_update_type_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("text"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // Update from bytes to int on MASTER
        client.setMode(Mode.MASTER);
        Variant previous = client.update(key, hint, "key1".getBytes(), vi(42)).get();
        assertEquals("text", new String(previous.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, hint, "key1".getBytes()).get();
        assertEquals(42, current.longValue());

        // Update from int to double on MASTER
        previous = client.update(key, hint, "key1".getBytes(), vd(3.14)).get();
        assertEquals(42, previous.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        current = client.setMode(Mode.BACKUP).getInContainer(key, hint, "key1".getBytes()).get();
        assertEquals(3.14, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("getAndDeleteValueInContainer on ordered map on MASTER, verify on BACKUP")
    void testGetAndDeleteValueInContainerOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_get_delete_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteValueInContainer on MASTER
        client.setMode(Mode.MASTER);
        Variant deleted = client.popInContainer(key, hint, "key1".getBytes()).get();
        assertNotNull(deleted);
        assertEquals("value1", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP
        Boolean exists = client.setMode(Mode.BACKUP).containsKey(key, hint, "key1".getBytes()).get();
        assertFalse(exists);

        exists = client.setMode(Mode.BACKUP).containsKey(key, hint, "key2".getBytes()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("removeInContainerByKey on ordered map on MASTER, verify on BACKUP")
    void testRemoveInContainerByKeyOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_remove_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // RemoveInContainerByKey on MASTER
        client.setMode(Mode.MASTER);
        Integer removed = client.remove(key, hint, "key1".getBytes()).get();
        assertEquals(1, removed);
        Thread.sleep(500);

        // Verify on BACKUP
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 3. ADD ELEMENT TO ORDERED MAP WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementOrderedMap on MASTER, validate on BACKUP")
    void testAddElementOrderedMapOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_add_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // AddElementOrderedMap on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.addElementOrderedMap(key, hint,
                                                    List.of(OrderedPayload.of("key2".getBytes(), 2L), OrderedPayload.of("key3".getBytes(), 3L)),
                                                    List.of(vi(100), vd(3.14))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        // Verify on BACKUP
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 4. RANGE QUERIES WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap on MASTER then BACKUP")
    void testStreamElementInRangeOrderedMapOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_range_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("1".getBytes(), 1L), v("one"));
        data.put(OrderedPayload.of("2".getBytes(), 2L), v("two"));
        data.put(OrderedPayload.of("3".getBytes(), 3L), v("three"));
        data.put(OrderedPayload.of("4".getBytes(), 4L), v("four"));
        data.put(OrderedPayload.of("5".getBytes(), 5L), v("five"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        Map<OrderedPayload, Variant> range = client.setMode(Mode.MASTER).getElementInRangeOrderedMap(key, hint, 2L, 4L, false).get();
        assertEquals(3, range.size());

        range = client.setMode(Mode.BACKUP).getElementInRangeOrderedMap(key, hint, 2L, 4L, false).get();
        assertEquals(3, range.size());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap with reverse on MASTER then BACKUP")
    void testStreamElementInRangeOrderedMapReverseOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_range_reverse_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("1".getBytes(), 1L), v("one"));
        data.put(OrderedPayload.of("2".getBytes(), 2L), v("two"));
        data.put(OrderedPayload.of("3".getBytes(), 3L), v("three"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        Map<OrderedPayload, Variant> range = client.setMode(Mode.MASTER).getElementInRangeOrderedMap(key, hint, 1L, 3L, true).get();
        assertEquals(3, range.size());

        range = client.setMode(Mode.BACKUP).getElementInRangeOrderedMap(key, hint, 1L, 3L, true).get();
        assertEquals(3, range.size());
    }

    // =========================================================================
    // 5. DELETE FROM ORDERED MAP WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("deleteFromContainerByKeyValue on ordered map on MASTER, verify on BACKUP")
    void testDeleteFromContainerByKeyValueOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_delete_value_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), v("val"));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), v("val"));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), v("other"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // DeleteFromContainerByKeyValue on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.ORDERED_MAP,
                List.of(OrderedPayload.of("k1".getBytes(), 1L), OrderedPayload.of("k2".getBytes(), 2L)),
                List.of(v("val"), v("val"))).get();
        assertEquals(2, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 6. SIZE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getSize on ordered map on MASTER then BACKUP")
    void testGetSizeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "ordered_map_size_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), v("v1"));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), v("v2"));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), v("v3"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
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
    @DisplayName("READ_LOCK on ordered map on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnOrderedMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_map_read_lock_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Map<OrderedPayload, Variant> val = client.setMode(Mode.BACKUP).getOrderedMap(key, hint, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        Map<OrderedPayload, Variant> intruderVal = client.setMode(Mode.BACKUP).getOrderedMap(key, hint, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on ordered map on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnOrderedMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_map_write_lock_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Map<OrderedPayload, Variant> val = client.setMode(Mode.BACKUP).getOrderedMap(key, hint, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.addElementOrderedMap(key, hint, 100,
                List.of(OrderedPayload.of("key2".getBytes(), 2L)),
                List.of(v("added"))).get();
        Thread.sleep(500);

        // Verify on BACKUP
        val = client.setMode(Mode.BACKUP).getOrderedMap(key, hint, 100).get();
        assertEquals(2, val.size());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on ordered map on MASTER: only owner can access")
    void testGlobalLockOnOrderedMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_map_global_lock_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Map<OrderedPayload, Variant> val = client.setMode(Mode.BACKUP).getOrderedMap(key, hint, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on ordered map with routing")
    void testLockExpirationOnOrderedMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "ordered_map_lock_expire_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP
        Map<OrderedPayload, Variant> val = client.setMode(Mode.BACKUP).getOrderedMap(key, hint, 100).get();
        assertNotNull(val);
    }
}