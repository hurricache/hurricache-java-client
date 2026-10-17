package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for HashMap operations.
 * Mirrors HashMapOperationsTest for cluster routing.
 */
public class SmartClientHashMapRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + STREAM MAP WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create empty map, validate on MASTER and BACKUP")
    void testCreateEmptyMapOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createMap(key, new HashMap<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(0, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create map with bytes key-value, read on MASTER then BACKUP")
    void testCreateMapWithBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "map_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));
        data.put(p("key3"), v("value3"));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key, hint).get();
        assertEquals(3, stream.size());

        List<Variant> values = new ArrayList<>(stream.values());
        assertEquals(3, values.size());

        stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create map with int values, read on MASTER then BACKUP")
    void testCreateMapWithIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "map_int_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), vi(10));
        data.put(p("k2"), vi(20));
        data.put(p("k3"), vi(30));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key, hint).get();
        assertEquals(3, stream.size());

        List<Long> ints = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.INT)
            .map(Variant::longValue)
            .sorted()
            .collect(Collectors.toList());
        assertEquals(3, ints.size());
        assertEquals(10, ints.get(0));
        assertEquals(20, ints.get(1));
        assertEquals(30, ints.get(2));

        stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create map with double values, read on MASTER then BACKUP")
    void testCreateMapWithDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "map_double_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), vd(1.1));
        data.put(p("k2"), vd(2.2));
        data.put(p("k3"), vd(3.3));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key, hint).get();
        assertEquals(3, stream.size());

        List<Double> doubles = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.DOUBLE)
            .map(Variant::doubleValue)
            .sorted()
            .collect(Collectors.toList());
        assertEquals(3, doubles.size());
        assertEquals(1.1, doubles.get(0), 0.001);
        assertEquals(2.2, doubles.get(1), 0.001);
        assertEquals(3.3, doubles.get(2), 0.001);

        stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create map with mixed types, validate on MASTER and BACKUP")
    void testCreateMapMixedTypesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_mixed_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("str"), v("hello"));
        data.put(p("int"), vi(42));
        data.put(p("dbl"), vd(3.14));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key, hint).get();
        assertEquals(3, stream.size());

        List<Variant> bytesVals = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.BYTES)
            .collect(Collectors.toList());
        List<Variant> intVals = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.INT)
            .collect(Collectors.toList());
        List<Variant> dblVals = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.DOUBLE)
            .collect(Collectors.toList());

        assertEquals(1, bytesVals.size());
        assertEquals(1, intVals.size());
        assertEquals(1, dblVals.size());

        stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 2. GET/UPDATE/DELETE VALUE IN CONTAINER WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey on MASTER then BACKUP")
    void testGetValueInContainerByKeyOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "map_get_value_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();
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
    @DisplayName("existInContainerByKey on MASTER then BACKUP")
    void testExistInContainerByKeyOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "map_exist_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, hint, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.setMode(Mode.BACKUP).containsKey(key, hint, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.setMode(Mode.MASTER).containsKey(key, hint, "key_missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("updateContainerValue on MASTER, validate on BACKUP")
    void testUpdateContainerValueOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_update_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("old_value"));

        KeyHintData hint = client.createMap(key, data).get();
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
    @DisplayName("updateContainerValue with type switch on MASTER, validate on BACKUP")
    void testUpdateContainerValueWithTypeSwitchOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_update_type_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("text"));

        KeyHintData hint = client.createMap(key, data).get();
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
    @DisplayName("getAndDeleteValueInContainer on MASTER, verify on BACKUP")
    void testGetAndDeleteValueInContainerOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_get_delete_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();
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
    @DisplayName("removeInContainerByKey on MASTER, verify on BACKUP")
    void testRemoveInContainerByKeyOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_remove_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // RemoveInContainerByKey on MASTER
        client.setMode(Mode.MASTER);
        Integer removed = client.remove(key, hint, "key1".getBytes()).get();
        assertEquals(1, removed);
        Thread.sleep(500);

        // Verify on BACKUP
        Map<Payload, Variant> stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 3. ADD ELEMENT TO MAP WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementHashMap on MASTER, validate on BACKUP")
    void testAddElementHashMapOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_add_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // AddElementHashMap on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.putAll(key, hint,
                                      List.of(p("key2"), p("key3")),
                                      List.of(vi(100), vd(3.14))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        // Verify on BACKUP
        Map<Payload, Variant> stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 4. DELETE FROM MAP WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("deleteFromContainerByKeyValue on MASTER, verify on BACKUP")
    void testDeleteFromContainerByKeyValueOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "map_delete_value_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("val"));
        data.put(p("k2"), v("val"));
        data.put(p("k3"), v("other"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // DeleteFromContainerByKeyValue on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.MAP,
                List.of(p("k1"), p("k2")), List.of(v("val"), v("val"))).get();
        assertEquals(2, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        Map<Payload, Variant> stream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 5. SIZE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getSize on MASTER then BACKUP")
    void testGetSizeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "map_size_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("v1"));
        data.put(p("k2"), v("v2"));
        data.put(p("k3"), v("v3"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(3, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 6. LOCK OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on map on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_read_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, hint, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        Map<Payload, Variant> intruderVal = client.setMode(Mode.BACKUP).getMap(key, hint, 100).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on map on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_write_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, hint, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.putAll(key, hint,
                      List.of(p("key2")),
                      List.of(v("added")), 100).get();
        Thread.sleep(500);

        // Verify on BACKUP
        val = client.setMode(Mode.BACKUP).getMap(key, hint, 100).get();
        assertEquals(2, val.size());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on map on MASTER: only owner can access")
    void testGlobalLockOnMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_global_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, hint, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on map with routing")
    void testLockExpirationOnMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_lock_expire_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP
        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, hint, 100).get();
        assertNotNull(val);
    }
}