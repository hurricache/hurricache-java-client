package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive cluster tests for HashedMap (MAP container) operations.
 * Each test verifies replication between MASTER and BACKUP nodes.
 */
public class HashMapOperationsTest extends TestBaseCluster {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final long REPLICATION_DELAY_MS = 100;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "hashmap_test_" + UUID.randomUUID();
    }

    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // Section 1: CREATE MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Creating empty HashedMap on master, verify replication to backup")
    void testCreateEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_empty";

        KeyHintData hint = client.createMap(mapKey, Map.of()).get();
        assertNotNull(hint, "KeyHint should be created");

        Thread.sleep(REPLICATION_DELAY_MS);

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Empty map should return empty map");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertNotNull(backupResult);
        assertEquals(0, backupResult.size());
    }

    @Test
    @DisplayName("Creating HashedMap with initial data on master, verify replication to backup")
    void testCreateMapWithInitialData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_initial";

        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2"),
                p("k3"), p("v3")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint, "KeyHint should be created");

        Thread.sleep(REPLICATION_DELAY_MS);

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Map should contain 3 elements");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertNotNull(backupResult);
        assertEquals(3, backupResult.size());
        assertTrue(backupResult.entrySet().stream()
                .anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("Creating large HashedMap with automatic chunking, verify 1500 elements on both nodes")
    void testCreateMapWithLargeDataChunking() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_large";
        int elementCount = 1500;
        Map<Payload, Payload> largeData = new java.util.HashMap<>();

        for (int i = 0; i < elementCount; i++) {
            largeData.put(p("key_" + i), p("value_" + i + "_" + UUID.randomUUID()));
        }

        KeyHintData hint = client.createMap(mapKey, largeData).get();
        assertNotNull(hint, "KeyHint should be created");

        // Large object - wait ~1s for replication
        Thread.sleep(1000);

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Map should contain " + elementCount + " elements");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertNotNull(backupResult);
        assertEquals(elementCount, backupResult.size());
    }

    // =========================================================================
    // Section 2: STREAM MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamMap of empty HashedMap on master, verify replication to backup")
    void testStreamMapEmpty() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_empty";

        KeyHintData hint = client.createMap(mapKey, Map.of()).get();
        assertNotNull(hint);
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamMap of empty map should return empty map");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertNotNull(backupResult);
        assertEquals(0, backupResult.size());
    }

    @Test
    @DisplayName("streamMap returns all container content on master, verify replication to backup")
    void testStreamMapWithData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_data";
        Map<Payload, Payload> initialData = Map.of(
                p("elem1"), p("val1"),
                p("elem2"), p("val2"),
                p("elem3"), p("val3"),
                p("elem4"), p("val4"),
                p("elem5"), p("val5")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint);

        Thread.sleep(REPLICATION_DELAY_MS);

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamMap should return all 5 elements");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertNotNull(backupResult);
        assertEquals(5, backupResult.size());
    }

    @Test
    @DisplayName("streamMap with explicit clientId on master, verify replication to backup")
    void testStreamMapWithClientId() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_cid";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<Payload, Payload> result = client.streamMap(bytes(mapKey), hint, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(1, result.size());

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertNotNull(backupResult);
        assertEquals(1, backupResult.size());
    }

    // =========================================================================
    // Section 3: ADD ELEMENT HASHMAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementHashMap adds elements on master, verify replication to backup")
    void testAddElementHashMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add";
        KeyHintData hint = client.createMap(mapKey, Map.of()).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        List<Payload> keys = List.of(p("k1"), p("k2"), p("k3"));
        List<Payload> values = List.of(p("v1"), p("v2"), p("v3"));
        Integer added = client.setMode(Mode.MASTER)
                .addElementHashMap(mapKey, hint, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added);

        // Verify on MASTER (no delay needed)
        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertEquals(3, result.size());

        // Verify on BACKUP (delay needed)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(3, backupResult.size());

        // Write on BACKUP
        List<Payload> keys2 = List.of(p("k4"));
        List<Payload> values2 = List.of(p("v4"));
        client.setMode(Mode.BACKUP)
                .addElementHashMap(mapKey, hint, keys2, values2, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        // Verify on BACKUP
        Map<Payload, Payload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(4, backupAfterWrite.size());

        // Verify on MASTER (delay needed)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamMap(mapKey, hint).get();
        assertEquals(4, masterAfterWrite.size());
    }

    @Test
    @DisplayName("addElementHashMap does not allow duplicate keys on master, verify on backup")
    void testAddElementHashMapNoDuplicates() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_nodup";
        KeyHintData hint = client.createMap(mapKey, Map.of()).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Add unique keys on MASTER
        List<Payload> keys1 = List.of(p("k1"), p("k2"));
        List<Payload> values1 = List.of(p("v1"), p("v2"));
        Integer added1 = client.setMode(Mode.MASTER)
                .addElementHashMap(mapKey, hint, keys1, values1, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(2, added1);

        // Try to add duplicate k1 - should return 0 on MASTER
        List<Payload> keys2 = List.of(p("k1"));
        List<Payload> values2 = List.of(p("v1_new"));
        Integer added = client.setMode(Mode.MASTER)
                .addElementHashMap(mapKey, hint, keys2, values2, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added, "Duplicate key should not be added");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(2, backupResult.size());
    }

    @Test
    @DisplayName("addElementHashMap with empty list returns 0 on master, verify on backup")
    void testAddElementHashMapEmptyList() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_empty";
        KeyHintData hint = client.createMap(mapKey, Map.of()).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        Integer added = client.setMode(Mode.MASTER)
                .addElementHashMap(mapKey, hint, List.of(), List.of(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added, "Adding empty list should return 0");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(0, backupResult.size());
    }

    // =========================================================================
    // Section 4: REMOVE FROM CONTAINER BY KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer removes element by key on master, verify replication to backup")
    void testRemoveFromContainerByKey() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2"),
                p("k3"), p("v3")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: remove k2
        Integer removed = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(mapKey), hint, p("k2").getValue()).get();
        assertEquals(1, removed, "1 element should be removed");

        // Verify on MASTER
        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertEquals(2, result.size(), "2 elements remain");
        assertFalse(result.containsKey(p("k2")));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(2, backupResult.size());
        assertFalse(backupResult.containsKey(p("k2")));
    }

    @Test
    @DisplayName("removeFromContainer returns 0 for non-existent key on master, verify on backup")
    void testRemoveFromContainerByKeyNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: remove non-existent
        Integer removed = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(mapKey), hint, p("nonexistent").getValue()).get();
        assertEquals(0, removed, "Nothing removed (element not found)");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(1, backupResult.size());
    }

    @Test
    @DisplayName("removeFromContainer from empty map returns 0 on master, verify on backup")
    void testRemoveFromContainerFromEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_empty";
        KeyHintData hint = client.createMap(mapKey, Map.of()).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Integer removed = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(mapKey), hint, p("k1").getValue()).get();
        assertEquals(0, removed, "Nothing removed from empty container");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(0, backupResult.size());
    }

    // =========================================================================
    // Section 5: REMOVE FROM CONTAINER WITH CONTAINERTYPE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer with ContainerType.MAP removes by key and value on master, verify on backup")
    void testRemoveFromContainerWithType() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        List<Payload> keys = List.of(Payload.of(p("k1").getValue()));
        List<Payload> values = List.of(Payload.of(p("v1").getValue()));
        Integer removed = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(mapKey), hint, ContainerType.MAP, keys, values).get();
        assertEquals(1, removed, "1 element should be removed");

        // Verify on MASTER
        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertEquals(1, result.size());
        assertFalse(result.containsKey(p("k1")));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(1, backupResult.size());
    }

    @Test
    @DisplayName("removeFromContainer with ContainerType.MAP removes by key even if value does not match on master, verify on backup")
    void testRemoveFromContainerWithTypeByWrongValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type_wv";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        List<Payload> keys = List.of(Payload.of(p("k1").getValue()));
        List<Payload> values = List.of(Payload.of(p("wrong_value").getValue()));
        Integer removed = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(mapKey), hint, ContainerType.MAP, keys, values).get();
        assertEquals(0, removed, "Element removed by key");

        // Verify on MASTER
        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertEquals(2, result.size(), "2 elements remain");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(2, backupResult.size());
    }

    // =========================================================================
    // Section 6: CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey checks existing key on master, verify on backup")
    void testContainsContainerKeyExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_exist";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Boolean exists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(mapKey), hint, p("k1").getValue()).get();
        assertTrue(exists, "Key k1 should exist");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(mapKey), hint, p("k1").getValue()).get();
        assertTrue(backupExists);
    }

    @Test
    @DisplayName("containsContainerKey checks non-existent key on master, verify on backup")
    void testContainsContainerKeyNotExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_notexist";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Boolean notExists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(mapKey), hint, p("nonexistent").getValue()).get();
        assertFalse(notExists, "Key nonexistent should not exist");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean backupNotExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(mapKey), hint, p("nonexistent").getValue()).get();
        assertFalse(backupNotExists);
    }

    // =========================================================================
    // Section 7: REMOVE CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("remove deletes existing container on master, verify NOT_FOUND on both nodes")
    void testRemoveExistingContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_cont";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Boolean removed = client.setMode(Mode.MASTER)
                .remove(bytes(mapKey), hint, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(removed, "Container should be removed");

        // Verify on MASTER
        try {
            client.streamMap(mapKey, hint).get();
            fail("streamMap of removed container should throw an error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP).streamMap(mapKey, hint).get();
            fail("streamMap of removed container on backup should throw an error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("remove of non-existent container returns NOT_FOUND on master")
    void testRemoveNonExistentContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_nonexist";

        // Write on MASTER
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // Section 8: GET SIZE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getSize for empty map on master, verify on backup")
    void testGetSizeEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_empty";
        KeyHintData hint = client.createMap(mapKey, Map.of()).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Integer size = client.setMode(Mode.MASTER)
                .getSize(mapKey, hint).get();
        assertEquals(0, size, "Empty map should have size 0");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(mapKey, hint).get();
        assertEquals(0, backupSize);
    }

    @Test
    @DisplayName("getSize for map with elements on master, verify on backup")
    void testGetSizeWithElements() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_with";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2"),
                p("k3"), p("v3")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Integer size = client.setMode(Mode.MASTER)
                .getSize(mapKey, hint).get();
        assertEquals(3, size, "Map should contain 3 elements");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(mapKey, hint).get();
        assertEquals(3, backupSize);
    }

    // =========================================================================
    // Section 9: GET AND UPDATE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("updateContainerValue updates value on master, verify replication to backup")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("old_value")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        byte[] elemKey = bytes("k1");
        byte[] oldValue = client.setMode(Mode.MASTER)
                .updateContainerValue(bytes(mapKey), hint, elemKey, p("new_value").getValue()).get();
        assertNotNull(oldValue, "oldValue should not be null");
        assertEquals("old_value", new String(oldValue, StandardCharsets.UTF_8));

        // Verify on MASTER
        byte[] newValue = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, elemKey).get();
        assertEquals("new_value", new String(newValue, StandardCharsets.UTF_8));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupNewValue = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(mapKey), hint, elemKey).get();
        assertEquals("new_value", new String(backupNewValue, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue for non-existent key returns NOT_FOUND on master")
    void testUpdateContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .updateContainerValue(bytes(mapKey), hint, p("nonexistent").getValue(), p("new").getValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("getContainerValue gets value by key on master, verify on backup")
    void testGetContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_val";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        byte[] value = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue()).get();
        assertNotNull(value);
        assertEquals("v1", new String(value, StandardCharsets.UTF_8));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupValue = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue()).get();
        assertNotNull(backupValue);
        assertEquals("v1", new String(backupValue, StandardCharsets.UTF_8));
    }

    // =========================================================================
    // Section 10: GET AND REMOVE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveContainerValue extracts and removes element on master, verify on backup")
    void testGetAndRemoveContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        byte[] removedValue = client.setMode(Mode.MASTER)
                .getAndRemoveContainerValue(bytes(mapKey), hint, p("k1").getValue()).get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue, StandardCharsets.UTF_8));

        // Verify on MASTER
        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertEquals(1, result.size(), "1 element remains");
        assertFalse(result.containsKey(p("k1")));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(1, backupResult.size());
        assertFalse(backupResult.containsKey(p("k1")));
    }

    @Test
    @DisplayName("getAndRemoveContainerValue for non-existent key returns NOT_FOUND on master")
    void testGetAndRemoveContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .getAndRemoveContainerValue(bytes(mapKey), hint, p("nonexistent").getValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // Section 11: GET CONTAINER VALUE OPERATIONS (additional tests)
    // =========================================================================

    @Test
    @DisplayName("getContainerValue for non-existent key returns NOT_FOUND on master")
    void testGetContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_val_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .getContainerValue(bytes(mapKey), hint, p("nonexistent").getValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // Section 12: TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl sets TTL on master, verify getTtl on both nodes")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_set";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        Boolean setResult = client.setMode(Mode.MASTER)
                .setTtl(mapKey, hint, 1000).get();
        assertTrue(setResult, "TTL should be successfully set");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(mapKey, hint).get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "TTL should be greater than 0 on backup");
    }

    @Test
    @DisplayName("getTtl gets container TTL on master, verify on backup")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_get";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER
        client.setMode(Mode.MASTER)
                .setTtl(mapKey, hint, 1000).get();

        // Verify on MASTER
        Long ttl = client.setMode(Mode.MASTER)
                .getTtl(mapKey, hint).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL should be greater than 0");

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(mapKey, hint).get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "TTL should be greater than 0 on backup");
    }

    @Test
    @DisplayName("After TTL expiration getContainerValue returns NOT_FOUND on master and backup")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_expire";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Set TTL = 1 second on MASTER
        client.setMode(Mode.MASTER)
                .setTtl(mapKey, hint, 1).get();

        // Wait for TTL expiration
        Thread.sleep(1500);

        // Verify on MASTER: getContainerValue will return NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(mapKey), hint, p("k1").getValue()).get();
            fail("getContainerValue should return NOT_FOUND after TTL expiration on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(mapKey), hint, p("k1").getValue()).get();
            fail("getContainerValue should return NOT_FOUND after TTL expiration on backup");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // Section 13: LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: multiple clients can read in parallel on master, verify on backup")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: acquire READ_LOCK
        LockStatus lock1 = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "First client should get READ_LOCK");

        LockStatus lock2 = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2, "Second client should get READ_LOCK");

        // Both can read on MASTER
        byte[] val1 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val1);

        byte[] val2 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val2);

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupVal1 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(backupVal1);

        // Release locks
        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, DEFAULT_CLIENT_ID).get();
        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: only owner reads and writes on master, others denied, verify on backup")
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: acquire WRITE_LOCK
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get WRITE_LOCK");

        // Owner can read on MASTER
        byte[] val = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Owner can add elements on MASTER
        List<Payload> keys = List.of(p("k2"));
        List<Payload> values = List.of(p("v2"));
        Integer added = client.setMode(Mode.MASTER)
                .addElementHashMap(bytes(mapKey), hint, keys, values, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, added);

        // Intruder cannot read on MASTER
        assertDenied(client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Intruder cannot add on MASTER
        List<Payload> intruderKeys = List.of(p("k3"));
        List<Payload> intruderValues = List.of(p("v3"));
        assertDenied(client.setMode(Mode.MASTER)
                .addElementHashMap(bytes(mapKey), hint, intruderKeys, intruderValues, INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint,OWNER_CLIENT_ID).get();
        assertEquals(2, backupResult.size());

        // Release lock
        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL: only owner can do any operations on master, all others blocked, verify on backup")
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_global_lock";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: acquire GLOBAL LOCK
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Owner should get GLOBAL LOCK");

        // Owner can read on MASTER
        byte[] val = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Intruder cannot read on MASTER
        assertDenied(client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Intruder cannot add on MASTER
        List<Payload> intruderKeys = List.of(p("k2"));
        List<Payload> intruderValues = List.of(p("v2"));
        assertDenied(client.setMode(Mode.MASTER)
                .addElementHashMap(bytes(mapKey), hint, intruderKeys, intruderValues, INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Intruder cannot unlock on MASTER
        LockStatus unlockStatus = client.setMode(Mode.MASTER)
                .unlockObject(mapKey, hint, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlockStatus);

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint,OWNER_CLIENT_ID).get();
        assertEquals(1, backupResult.size());

        // Release lock by owner
        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("unlockObject: only owner can unlock on master, verify on backup")
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unlock_owner";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: acquire WRITE_LOCK
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder tries to unlock on MASTER
        LockStatus status = client.setMode(Mode.MASTER)
                .unlockObject(mapKey, hint, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, status);

        // Owner unlocks on MASTER
        LockStatus validUnlock = client.setMode(Mode.MASTER)
                .unlockObject(mapKey, hint, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock);

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(1, backupResult.size());
    }

    @Test
    @DisplayName("READ_LOCK: read OK, but write is denied on master, verify on backup")
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_writes";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Write on MASTER: acquire READ_LOCK
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Read under READ_LOCK OK on MASTER
        byte[] val = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Write is denied on MASTER
        List<Payload> keys = List.of(p("k2"));
        List<Payload> values = List.of(p("v2"));
        assertDenied(client.setMode(Mode.MASTER)
                .addElementHashMap(bytes(mapKey), hint, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint).get();
        assertEquals(1, backupResult.size());

        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, DEFAULT_CLIENT_ID).get();
    }

    // =========================================================================
    // Section 14: LOCK EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK for 2s: after expiration another client can read on master and backup")
    void testReadLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_exp";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Acquire READ_LOCK for 2 seconds on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now another client can read on MASTER
        byte[] val = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);
        assertEquals("v1", new String(val, StandardCharsets.UTF_8));

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupVal = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(mapKey), hint, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(backupVal);
        assertEquals("v1", new String(backupVal, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("WRITE_LOCK for 2s: after expiration another client can acquire WRITE_LOCK on master and backup")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_exp";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Acquire WRITE_LOCK for 2 seconds on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK on MASTER
        LockStatus newLock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint,INTRUDER_CLIENT_ID).get();
        assertEquals(1, backupResult.size());

        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL for 2s: after expiration another client can acquire WRITE_LOCK on master and backup")
    void testGlobalLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_global_lock_exp";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Acquire GLOBAL LOCK for 2 seconds on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK on MASTER
        LockStatus newLock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint, INTRUDER_CLIENT_ID).get();
        assertEquals(1, backupResult.size());

        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Any lock for 2s: after expiration new lock OK for another client on master and backup")
    void testLockExpirationAllowsNewLock() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_lock_exp_any";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Acquire READ_LOCK for 2 seconds on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Wait for expiration
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK on MASTER
        LockStatus newLock = client.setMode(Mode.MASTER)
                .lockObject(mapKey, hint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        // Verify on BACKUP
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<Payload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, hint, INTRUDER_CLIENT_ID).get();
        assertEquals(1, backupResult.size());

        client.setMode(Mode.MASTER).unlockObject(mapKey, hint, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // Section 15: UNSUPPORTED METHODS FOR MAP
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to MAP should throw an error on master")
    void testUnsupportedMethodsForMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unsupported";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        KeyHintData hint = client.createMap(mapKey, initialData).get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getElementAtPosition — not applicable to MAP
        try {
            client.setMode(Mode.MASTER)
                    .getElementAtPosition(mapKey, hint, 0).get();
            fail("getElementAtPosition should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getAndRemoveElementAtPosition — not applicable to MAP
        try {
            client.setMode(Mode.MASTER)
                    .getAndRemoveElementAtPosition(mapKey, hint, 0).get();
            fail("getAndRemoveElementAtPosition should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getHead — not applicable to MAP
        try {
            client.setMode(Mode.MASTER)
                    .getHead(mapKey, hint).get();
            fail("getHead should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail — not applicable to MAP
        try {
            client.setMode(Mode.MASTER)
                    .getTail(mapKey, hint).get();
            fail("getTail should throw an error for MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }
    }
}