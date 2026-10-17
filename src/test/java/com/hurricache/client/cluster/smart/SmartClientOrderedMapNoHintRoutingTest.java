package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class SmartClientOrderedMapNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("Create empty ordered map, read on MASTER without keyHint")
    void testCreateEmptyOrderedMapWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_empty_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedMap(key, new HashMap<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create empty ordered map, read on BACKUP without keyHint")
    void testCreateEmptyOrderedMapWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_empty_b_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedMap(key, new HashMap<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create ordered map with bytes, read on MASTER without keyHint")
    void testCreateOrderedMapWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_bytes_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));
        data.put(OrderedPayload.of("key3".getBytes(), 3L), v("value3"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with bytes, read on BACKUP without keyHint")
    void testCreateOrderedMapWithBytesWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_bytes_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));
        data.put(OrderedPayload.of("key3".getBytes(), 3L), v("value3"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.BACKUP).getOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with int, read on MASTER without keyHint")
    void testCreateOrderedMapWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_int_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), vi(10));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vi(20));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), vi(30));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered map with int, read on BACKUP without keyHint")
    void testCreateOrderedMapWithIntWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_int_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), vi(10));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vi(20));
        data.put(OrderedPayload.of("k3".getBytes(), 3L), vi(30));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.BACKUP).getOrderedMap(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Get value in ordered map container on MASTER without keyHint")
    void testGetValueInOrderedMapContainerOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_get_value_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Variant value = client.setMode(Mode.MASTER).getInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(value);
        assertEquals("value1", new String(value.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Get value in ordered map container on BACKUP without keyHint")
    void testGetValueInOrderedMapContainerOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_get_value_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Variant value = client.setMode(Mode.BACKUP).getInContainer(key, null, "key2".getBytes()).get();
        assertNotNull(value);
        assertEquals(100, value.longValue());
    }

    @Test
    @DisplayName("Check ordered map contains on MASTER without keyHint")
    void testOrderedMapContainsOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_contains_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, null, "key1".getBytes()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("Check ordered map contains on BACKUP without keyHint")
    void testOrderedMapContainsOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_contains_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Boolean exists = client.setMode(Mode.BACKUP).containsKey(key, null, "key1".getBytes()).get();
        assertTrue(exists);
        exists = client.setMode(Mode.MASTER).containsKey(key, null, "missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("Update value in ordered map container on MASTER without keyHint")
    void testUpdateValueInOrderedMapContainerOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_update_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("old_value"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
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
    @DisplayName("Update value in ordered map container on BACKUP without keyHint")
    void testUpdateValueInOrderedMapContainerOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_update_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("old"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        client.update(key, null, "key1".getBytes(), v("intermediate")).get();
        Thread.sleep(500);
        Variant previous = client.setMode(Mode.BACKUP).update(key, null, "key1".getBytes(), v("final")).get();
        assertEquals("intermediate", new String(previous.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Get and delete value in ordered map container on MASTER without keyHint")
    void testGetAndDeleteValueInOrderedMapContainerOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_get_delete_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), vi(100));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        Variant deleted = client.popInContainer(key, null, "key1".getBytes()).get();
        assertNotNull(deleted);
        assertEquals("value1", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(1, size);
    }

    @Test
    @DisplayName("Get ordered map size on MASTER without keyHint")
    void testOrderedMapSizeOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_size_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("val1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("val2"));
        data.put(OrderedPayload.of("key3".getBytes(), 3L), v("val3"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Get ordered map size on BACKUP without keyHint")
    void testOrderedMapSizeOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_size_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("val1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("val2"));
        data.put(OrderedPayload.of("key3".getBytes(), 3L), v("val3"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Stream ordered map on MASTER without keyHint")
    void testStreamOrderedMapOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_stream_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("val1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("val2"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.MASTER).getOrderedMap(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("Stream ordered map on BACKUP without keyHint")
    void testStreamOrderedMapOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_stream_b_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("val1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("val2"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        Map<OrderedPayload, Variant> stream = client.setMode(Mode.BACKUP).getOrderedMap(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("Update value with type switch on ordered map on MASTER without keyHint")
    void testUpdateValueWithTypeSwitchOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "omap_no_hint_type_switch_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("text"));
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        Variant previous = client.update(key, null, "key1".getBytes(), vi(42)).get();
        assertEquals("text", new String(previous.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, null, "key1".getBytes()).get();
        assertEquals(42, current.longValue());
    }
}