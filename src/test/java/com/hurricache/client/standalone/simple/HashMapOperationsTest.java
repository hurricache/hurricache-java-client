package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashMapOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + STREAM MAP
    // =========================================================================

    @Test
    @DisplayName("Create empty map")
    void testCreateEmptyMap() throws ExecutionException, InterruptedException {
        String key = "map_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createMap(key, new HashMap<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create map with bytes key-value")
    void testCreateMapWithBytes() throws ExecutionException, InterruptedException {
        String key = "map_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));
        data.put(p("key3"), v("value3"));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(3, stream.size());

        // Collect values and check by type/content
        List<Variant> values = new ArrayList<>(stream.values());
        assertEquals(3, values.size());

        // Find "value1" among the values
        Variant found = values.stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.BYTES)
            .filter(v -> new String(v.bytesValue(), java.nio.charset.StandardCharsets.UTF_8).equals("value1"))
            .findFirst()
            .orElse(null);
        assertNotNull(found);
    }

    @Test
    @DisplayName("Create map with int values")
    void testCreateMapWithInt() throws ExecutionException, InterruptedException {
        String key = "map_int_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), vi(10));
        data.put(p("k2"), vi(20));
        data.put(p("k3"), vi(30));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(3, stream.size());

        // Collect values by type
        List<Long> ints = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.INT)
            .map(Variant::longValue)
            .sorted()
            .collect(Collectors.toList());
        assertEquals(3, ints.size());
        assertEquals(10, ints.get(0));
        assertEquals(20, ints.get(1));
        assertEquals(30, ints.get(2));
    }

    @Test
    @DisplayName("Create map with double values")
    void testCreateMapWithDouble() throws ExecutionException, InterruptedException {
        String key = "map_double_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), vd(1.1));
        data.put(p("k2"), vd(2.2));
        data.put(p("k3"), vd(3.3));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(3, stream.size());

        // Collect double values
        List<Double> doubles = stream.values().stream()
            .filter(v -> v.type() == com.hurricache.client.intf.Variant.Type.DOUBLE)
            .map(Variant::doubleValue)
            .sorted()
            .collect(Collectors.toList());
        assertEquals(3, doubles.size());
        assertEquals(1.1, doubles.get(0), 0.001);
        assertEquals(2.2, doubles.get(1), 0.001);
        assertEquals(3.3, doubles.get(2), 0.001);
    }

    @Test
    @DisplayName("Create map with mixed types")
    void testCreateMapMixedTypes() throws ExecutionException, InterruptedException {
        String key = "map_mixed_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("str"), v("hello"));
        data.put(p("int"), vi(42));
        data.put(p("dbl"), vd(3.14));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(3, stream.size());

        // Collect values by type
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

        assertEquals("hello", new String(bytesVals.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(42, intVals.get(0).longValue());
        assertEquals(3.14, dblVals.get(0).doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. GET/UPDATE/DELETE VALUE IN CONTAINER
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey")
    void testGetValueInContainer() throws ExecutionException, InterruptedException {
        String key = "map_get_value_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();

        Variant value = client.getValueInContainerByKey(key, null, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        value = client.getValueInContainerByKey(key, null, "key2".getBytes()).get();
        assertNotNull(value);
        assertEquals(100, value.longValue());
    }

    @Test
    @DisplayName("existInContainerByKey")
    void testExistInContainer() throws ExecutionException, InterruptedException {
        String key = "map_exist_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();

        Boolean exists = client.existInContainerByKey(key, null, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.existInContainerByKey(key, null, "key_missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("updateContainerValue")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String key = "map_update_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("old_value"));

        KeyHintData hint = client.createMap(key, data).get();

        Variant previous = client.updateContainerValue(key, null, "key1".getBytes(), v("new_value")).get();
        assertNotNull(previous);
        assertEquals("old_value", new String(previous.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant current = client.getValueInContainerByKey(key, null, "key1".getBytes()).get();
        assertEquals("new_value", new String(current.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue with type switch")
    void testUpdateContainerValueWithTypeSwitch() throws ExecutionException, InterruptedException {
        String key = "map_update_type_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("text"));

        KeyHintData hint = client.createMap(key, data).get();

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
    @DisplayName("getAndDeleteValueInContainer")
    void testGetAndDeleteValueInContainer() throws ExecutionException, InterruptedException {
        String key = "map_get_delete_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();

        Variant deleted = client.getAndDeleteValueInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(deleted);
        assertEquals("value1", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Boolean exists = client.existInContainerByKey(key, null, "key1".getBytes()).get();
        assertFalse(exists);

        exists = client.existInContainerByKey(key, null, "key2".getBytes()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("removeInContainerByKey")
    void testRemoveInContainerByKey() throws ExecutionException, InterruptedException {
        String key = "map_remove_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));

        KeyHintData hint = client.createMap(key, data).get();

        Integer removed = client.removeInContainerByKey(key, null, "key1".getBytes()).get();
        assertEquals(1, removed);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 3. ADD ELEMENT TO MAP
    // =========================================================================

    @Test
    @DisplayName("addElementHashMap")
    void testAddElementHashMap() throws ExecutionException, InterruptedException {
        String key = "map_add_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();

        Integer added = client.addElementHashMap(key, hint,
                                                 List.of(p("key2"), p("key3")),
                                                 List.of(vi(100), vd(3.14))).get();
        assertEquals(2, added);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(3, stream.size());
    }

    // =========================================================================
    // 4. DELETE FROM MAP
    // =========================================================================

    @Test
    @DisplayName("deleteFromContainerByKeyValue")
    void testDeleteFromContainerByKeyValue() throws ExecutionException, InterruptedException {
        String key = "map_delete_value_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("val"));
        data.put(p("k2"), v("val"));
        data.put(p("k3"), v("other"));

        KeyHintData hint = client.createMap(key, data).get();

        // Delete by both key AND value - delete k1 and k2 which have "val"
        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.MAP,
                List.of(p("k1"), p("k2")), List.of(v("val"), v("val"))).get();
        assertEquals(2, deleted);

        Map<Payload, Variant> stream = client.streamMap(key).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 5. SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "map_size_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("v1"));
        data.put(p("k2"), v("v2"));
        data.put(p("k3"), v("v3"));

        KeyHintData hint = client.createMap(key, data).get();

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 6. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on map: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "map_read_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Map<Payload, Variant> val = client.streamMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Map<Payload, Variant> intruderVal = client.streamMap(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.addElementHashMap(key, hint, List.of(p("key2")), List.of(v("value2")), OWNER_CLIENT_ID));
        assertDenied(client.addElementHashMap(key, hint, List.of(p("key2")), List.of(v("value2")), INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on map: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "map_write_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read and write
        Map<Payload, Variant> val = client.streamMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        client.addElementHashMap(key, hint,
                List.of(p("key2")),
                List.of(v("added")),
                OWNER_CLIENT_ID).get();

        // Intruder denied - use INTRUDER_CLIENT_ID explicitly
        assertDenied(client.streamMap(key, null, INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on map: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "map_global_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can access
        Map<Payload, Variant> val = client.streamMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder denied - use INTRUDER_CLIENT_ID explicitly
        assertDenied(client.streamMap(key, null, INTRUDER_CLIENT_ID));

        client.unlockObject(key, OWNER_CLIENT_ID).get();
    }
    @Test
    @DisplayName("Lock expiration on map")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "map_lock_expire_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lockObject(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        // Lock should expire, intruder can access
        Map<Payload, Variant> val = client.streamMap(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}