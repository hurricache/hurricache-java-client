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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for HashMap operations WITHOUT keyHint.
 */
public class SmartClientHashMapNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("Create empty map, read on MASTER without keyHint")
    void testCreateEmptyMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createMap(key, new HashMap<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create map with bytes key-value, read on MASTER without keyHint")
    void testCreateMapWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));
        data.put(p("key3"), v("value3"));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create map with int values, read on MASTER without keyHint")
    void testCreateMapWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_int_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), vi(10));
        data.put(p("k2"), vi(20));
        data.put(p("k3"), vi(30));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key).get();
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
    }

    @Test
    @DisplayName("Create map with double values, read on MASTER without keyHint")
    void testCreateMapWithDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_double_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), vd(1.1));
        data.put(p("k2"), vd(2.2));
        data.put(p("k3"), vd(3.3));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create map with mixed types, read on MASTER without keyHint")
    void testCreateMapMixedTypesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_mixed_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("str"), v("hello"));
        data.put(p("int"), vi(42));
        data.put(p("dbl"), vd(3.14));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.MASTER).getMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("getValueInContainerByKey on MASTER without keyHint")
    void testGetValueInContainerByKeyWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_get_value_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        Variant value = client.setMode(Mode.MASTER).getInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), StandardCharsets.UTF_8));

        value = client.setMode(Mode.BACKUP).getInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("existInContainerByKey on MASTER without keyHint")
    void testExistInContainerByKeyWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_exist_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, null, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.setMode(Mode.BACKUP).containsKey(key, null, "key1".getBytes()).get();
        assertTrue(exists);

        exists = client.setMode(Mode.MASTER).containsKey(key, null, "key_missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("updateContainerValue on MASTER without keyHint")
    void testUpdateContainerValueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_update_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("old_value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Variant previous = client.update(key, null, "key1".getBytes(), v("new_value")).get();
        assertNotNull(previous);
        assertEquals("old_value", new String(previous.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, null, "key1".getBytes()).get();
        assertEquals("new_value", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteValueInContainer on MASTER without keyHint")
    void testGetAndDeleteValueInContainerWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_get_delete_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Variant deleted = client.popInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(deleted);
        assertEquals("value1", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        Boolean exists = client.setMode(Mode.BACKUP).containsKey(key, null, "key1".getBytes()).get();
        assertFalse(exists);

        exists = client.setMode(Mode.BACKUP).containsKey(key, null, "key2".getBytes()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("removeInContainerByKey on MASTER without keyHint")
    void testRemoveInContainerByKeyWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_remove_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer removed = client.remove(key, null, "key1".getBytes()).get();
        assertEquals(1, removed);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.BACKUP).getMap(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("addElementHashMap on MASTER without keyHint")
    void testAddElementHashMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_add_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer added = client.putAll(key, List.of(p("key2"), p("key3")), List.of(vi(100), vd(3.14))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.BACKUP).getMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue on MASTER without keyHint")
    void testDeleteFromContainerByKeyValueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_delete_value_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("val"));
        data.put(p("k2"), v("val"));
        data.put(p("k3"), v("other"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.MAP, List.of(p("k1"), p("k2")), List.of(v("val"), v("val"))).get();
        assertEquals(2, deleted);
        Thread.sleep(500);

        Map<Payload, Variant> stream = client.setMode(Mode.BACKUP).getMap(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("getSize on map on MASTER without keyHint")
    void testGetSizeWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_size_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("v1"));
        data.put(p("k2"), v("v2"));
        data.put(p("k3"), v("v3"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("READ_LOCK on map on MASTER without keyHint")
    void testReadLockOnMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_read_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, null, 100).get();
        assertNotNull(val);

        Map<Payload, Variant> intruderVal = client.setMode(Mode.BACKUP).getMap(key, null, 100).get();
        assertNotNull(intruderVal);

        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on map on MASTER without keyHint")
    void testWriteLockOnMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_write_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, null, 100).get();
        assertNotNull(val);

        client.putAll(key, List.of(p("key2")), List.of(v("added")), 100).get();
        Thread.sleep(500);

        val = client.setMode(Mode.BACKUP).getMap(key, null, 100).get();
        assertEquals(2, val.size());

        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on map on MASTER without keyHint")
    void testGlobalLockOnMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_global_lock_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, null, 100).get();
        assertNotNull(val);

        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on map without keyHint")
    void testLockExpirationOnMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "map_no_hint_lock_expire_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        Map<Payload, Variant> val = client.setMode(Mode.BACKUP).getMap(key, null, 100).get();
        assertNotNull(val);
    }
}