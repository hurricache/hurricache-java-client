package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OrderedMapOperationsTest extends TestBaseCluster {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final long REPLICATION_DELAY_MS = 150;

    private OrderedPayload op(Long order, String val) {
        return OrderedPayload.of(order, val.getBytes(StandardCharsets.UTF_8));
    }

    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 1. CREATE ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Create empty ordered map, verify replication to backup")
    void testCreateEmptyOrderedMap() throws ExecutionException, InterruptedException {
        String key = "emptyOrderedMap" + UUID.randomUUID();

        // Create empty ordered map
        KeyHintData keyHint = client.createOrderedMap(key, Map.of())
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getSize returns 0
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertNotNull(masterSize);
        assertEquals(0, masterSize, "Expected size 0 on master");

        // Verify on BACKUP: getSize returns 0
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertNotNull(backupSize);
        assertEquals(0, backupSize, "Expected size 0 on backup");
    }

    @Test
    @DisplayName("Create ordered map with data, verify replication to backup")
    void testCreateOrderedMapWithData() throws ExecutionException, InterruptedException {
        String key = "orderedMapWithData" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getSize returns 3
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterSize);

        // Verify on MASTER: getContainerValue returns correct values
        byte[] masterVal1 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertNotNull(masterVal1);
        assertEquals("v1", new String(masterVal1, StandardCharsets.UTF_8));

        byte[] masterVal2 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k2"))
                .get();
        assertNotNull(masterVal2);
        assertEquals("v2", new String(masterVal2, StandardCharsets.UTF_8));

        byte[] masterVal3 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k3"))
                .get();
        assertNotNull(masterVal3);
        assertEquals("v3", new String(masterVal3, StandardCharsets.UTF_8));

        // Verify on BACKUP: getSize returns 3
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, backupSize);

        // Verify on BACKUP: getContainerValue returns correct values
        byte[] backupVal1 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertNotNull(backupVal1);
        assertEquals("v1", new String(backupVal1, StandardCharsets.UTF_8));

        byte[] backupVal2 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k2"))
                .get();
        assertNotNull(backupVal2);
        assertEquals("v2", new String(backupVal2, StandardCharsets.UTF_8));

        byte[] backupVal3 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k3"))
                .get();
        assertNotNull(backupVal3);
        assertEquals("v3", new String(backupVal3, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create large ordered map with chunking, verify 1500 elements on both nodes")
    void testCreateLargeOrderedMapWithChunking() throws ExecutionException, InterruptedException {
        String key = "largeOrderedMap" + UUID.randomUUID();
        int elementCount = 1500;

        Map<OrderedPayload, Payload> payloads = new java.util.LinkedHashMap<>();
        for (int i = 0; i < elementCount; i++) {
            payloads.put(op((long) i, "key_" + i), p("value_" + i));
        }

        // Create large ordered map
        KeyHintData keyHint = client.createOrderedMap(key, payloads)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getSize returns 1500
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(elementCount, masterSize, "Expected " + elementCount + " elements on master");

        // Verify on BACKUP: getSize returns 1500
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(elementCount, backupSize, "Expected " + elementCount + " elements on backup");

        // Verify integrity: first and last elements match on MASTER
        byte[] masterFirst = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("key_0"))
                .get();
        assertNotNull(masterFirst);
        assertEquals("value_0", new String(masterFirst, StandardCharsets.UTF_8));

        byte[] masterLast = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("key_" + (elementCount - 1)))
                .get();
        assertNotNull(masterLast);
        assertEquals("value_" + (elementCount - 1), new String(masterLast, StandardCharsets.UTF_8));

        // Verify integrity: first and last elements match on BACKUP
        byte[] backupFirst = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("key_0"))
                .get();
        assertNotNull(backupFirst);
        assertEquals("value_0", new String(backupFirst, StandardCharsets.UTF_8));

        byte[] backupLast = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("key_" + (elementCount - 1)))
                .get();
        assertNotNull(backupLast);
        assertEquals("value_" + (elementCount - 1), new String(backupLast, StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 2. ADD ELEMENT ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementOrderedMap on master, verify replication to backup")
    void testAddElementOrderedMapOnMaster() throws ExecutionException, InterruptedException {
        String key = "addElementMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupInitial);

        // addElementOrderedMap on MASTER
        List<OrderedPayload> newElements = List.of(
                op(2L, "k2"),
                op(3L, "k3")
        );
        List<Payload> newValues = List.of(
                p("v2"),
                p("v3")
        );

        Integer added = client.setMode(Mode.MASTER)
                .addElementOrderedMap(bytes(key), keyHint, newElements, newValues)
                .get();
        assertEquals(2, added);

        // Verify on MASTER: getSize returns 3
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterSize);

        // Verify on BACKUP: getSize returns 3 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, backupSize);

        // Verify values on BACKUP
        byte[] backupVal2 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k2"))
                .get();
        assertNotNull(backupVal2);
        assertEquals("v2", new String(backupVal2, StandardCharsets.UTF_8));

        byte[] backupVal3 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k3"))
                .get();
        assertNotNull(backupVal3);
        assertEquals("v3", new String(backupVal3, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementOrderedMap on backup, verify replication to master")
    void testAddElementOrderedMapOnBackup() throws ExecutionException, InterruptedException {
        String key = "addElementBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupInitial);

        // addElementOrderedMap on BACKUP
        List<OrderedPayload> newElements = List.of(
                op(2L, "k2"),
                op(3L, "k3")
        );
        List<Payload> newValues = List.of(
                p("v2"),
                p("v3")
        );

        Integer added = client.setMode(Mode.BACKUP)
                .addElementOrderedMap(bytes(key), keyHint, newElements, newValues)
                .get();
        assertEquals(2, added);

        // Verify on BACKUP: getSize returns 3
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, backupSize);

        // Verify on MASTER: getSize returns 3 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterSize);

        // Verify values on MASTER
        byte[] masterVal2 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k2"))
                .get();
        assertNotNull(masterVal2);
        assertEquals("v2", new String(masterVal2, StandardCharsets.UTF_8));

        byte[] masterVal3 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k3"))
                .get();
        assertNotNull(masterVal3);
        assertEquals("v3", new String(masterVal3, StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. UPDATE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("updateContainerValue on master, verify replication to backup")
    void testUpdateContainerValueOnMaster() throws ExecutionException, InterruptedException {
        String key = "updateContainerValueOnMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        byte[] masterVal1 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("v1", new String(masterVal1, StandardCharsets.UTF_8));

        byte[] backupVal1 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("v1", new String(backupVal1, StandardCharsets.UTF_8));

        // updateContainerValue on MASTER
        byte[] oldVal = client.setMode(Mode.MASTER)
                .updateContainerValue(bytes(key), keyHint, bytes("k1"), bytes("updated_v1"))
                .get();
        assertEquals("v1", new String(oldVal, StandardCharsets.UTF_8));

        // Verify on MASTER: getContainerValue returns updated value
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterUpdated = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("updated_v1", new String(masterUpdated, StandardCharsets.UTF_8));

        // Verify on BACKUP: getContainerValue returns updated value (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupUpdated = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("updated_v1", new String(backupUpdated, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue on backup, verify replication to master")
    void testUpdateContainerValueOnBackup() throws ExecutionException, InterruptedException {
        String key = "updateContainerValueOnBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        byte[] masterVal1 = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("v1", new String(masterVal1, StandardCharsets.UTF_8));

        byte[] backupVal1 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("v1", new String(backupVal1, StandardCharsets.UTF_8));

        // updateContainerValue on BACKUP
        byte[] oldVal = client.setMode(Mode.BACKUP)
                .updateContainerValue(bytes(key), keyHint, bytes("k1"), bytes("updated_v1"))
                .get();
        assertEquals("v1", new String(oldVal, StandardCharsets.UTF_8));

        // Verify on BACKUP: getContainerValue returns updated value
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupUpdated = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("updated_v1", new String(backupUpdated, StandardCharsets.UTF_8));

        // Verify on MASTER: getContainerValue returns updated value (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterUpdated = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertEquals("updated_v1", new String(masterUpdated, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue for non-existent key returns NOT_FOUND")
    void testUpdateContainerValueNotFound() throws ExecutionException, InterruptedException {
        String key = "updateContainerValueNotFound" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // updateContainerValue for non-existent key on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .updateContainerValue(bytes(key), keyHint, bytes("nonexistent"), bytes("new_value"))
                    .get();
            fail("Expected NOT_FOUND for non-existent key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: also fails
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .updateContainerValue(bytes(key), keyHint, bytes("nonexistent"), bytes("new_value"))
                    .get();
            fail("Expected NOT_FOUND for non-existent key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 4. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey on master, verify key exists on both nodes")
    void testContainsContainerKeyOnMaster() throws ExecutionException, InterruptedException {
        String key = "containsKeyMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // containsContainerKey on MASTER
        Boolean existsMaster = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, bytes("k1"))
                .get();
        assertTrue(existsMaster);

        // containsContainerKey on BACKUP (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean existsBackup = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, bytes("k1"))
                .get();
        assertTrue(existsBackup);

        // Non-existent key on both nodes
        Boolean notExistsMaster = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, bytes("nonexistent"))
                .get();
        assertFalse(notExistsMaster);

        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean notExistsBackup = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, bytes("nonexistent"))
                .get();
        assertFalse(notExistsBackup);
    }

    @Test
    @DisplayName("containsContainerKey on backup, verify key exists on both nodes")
    void testContainsContainerKeyOnBackup() throws ExecutionException, InterruptedException {
        String key = "containsKeyBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // containsContainerKey on BACKUP
        Boolean existsBackup = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, bytes("k1"))
                .get();
        assertTrue(existsBackup);

        // containsContainerKey on MASTER (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean existsMaster = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, bytes("k1"))
                .get();
        assertTrue(existsMaster);
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer on master, verify replication to backup")
    void testRemoveFromContainerOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, backupInitial);

        // removeFromContainer on MASTER
        Integer removed = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(key), keyHint, bytes("k1"))
                .get();
        assertTrue(removed >= 1);

        // Verify on MASTER: getSize returns 2
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Verify on BACKUP: getSize returns 2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        // Verify k1 is gone on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND for removed key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("removeFromContainer on backup, verify replication to master")
    void testRemoveFromContainerOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, backupInitial);

        // removeFromContainer on BACKUP
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(bytes(key), keyHint, bytes("k1"))
                .get();
        assertTrue(removed >= 1);

        // Verify on BACKUP: getSize returns 2
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        // Verify on MASTER: getSize returns 2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Verify k1 is gone on MASTER
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND for removed key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 6. REMOVE ELEMENT AT POSITION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition on master, verify replication to backup")
    void testRemoveElementAtPositionOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPositionMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3"),
                op(4L, "k4"), p("v4")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, backupInitial);

        // removeElementAtPosition on MASTER (remove weights 2-3)
        client.setMode(Mode.MASTER)
                .removeElementAtPosition(bytes(key), keyHint, 2L, 3L)
                .get();

        // Verify on MASTER: getSize returns 2
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Verify on BACKUP: getSize returns 2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        // Verify k2, k3 are gone on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k2"))
                    .get();
            fail("Expected NOT_FOUND for removed element");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("removeElementAtPosition on backup, verify replication to master")
    void testRemoveElementAtPositionOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPositionBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3"),
                op(4L, "k4"), p("v4")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, backupInitial);

        // removeElementAtPosition on BACKUP (remove weights 2-3)
        client.setMode(Mode.BACKUP)
                .removeElementAtPosition(bytes(key), keyHint, 2L, 3L)
                .get();

        // Verify on BACKUP: getSize returns 2
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        // Verify on MASTER: getSize returns 2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Verify k2, k3 are gone on MASTER
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(key), keyHint, bytes("k2"))
                    .get();
            fail("Expected NOT_FOUND for removed element");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 7. STREAM ELEMENT IN RANGE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap on master, verify replication to backup")
    void testStreamElementInRangeOnMaster() throws ExecutionException, InterruptedException {
        String key = "streamElementInRangeMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3"),
                op(4L, "k4"), p("v4")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // streamElementInRangeOrderedMap on MASTER (weights 1-2)
        Map<OrderedPayload, Payload> streamResult = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 1L, 2L, false, OWNER_CLIENT_ID)
                .get();
        assertEquals(2, streamResult.size());
        assertTrue(streamResult.containsKey(op(1L, "k1")));
        assertTrue(streamResult.containsKey(op(2L, "k2")));

        // Verify on BACKUP: streamElementInRangeOrderedMap returns same elements (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 1L, 2L, false, OWNER_CLIENT_ID)
                .get();
        assertEquals(2, backupStream.size());
        assertTrue(backupStream.containsKey(op(1L, "k1")));
        assertTrue(backupStream.containsKey(op(2L, "k2")));
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap on backup, verify replication to master")
    void testStreamElementInRangeOnBackup() throws ExecutionException, InterruptedException {
        String key = "streamElementInRangeBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3"),
                op(4L, "k4"), p("v4")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // streamElementInRangeOrderedMap on BACKUP (weights 3-4)
        Map<OrderedPayload, Payload> streamResult = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 3L, 4L, false, OWNER_CLIENT_ID)
                .get();
        assertEquals(2, streamResult.size());
        assertTrue(streamResult.containsKey(op(3L, "k3")));
        assertTrue(streamResult.containsKey(op(4L, "k4")));

        // Verify on MASTER: streamElementInRangeOrderedMap returns same elements (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> masterStream = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 3L, 4L, false, OWNER_CLIENT_ID)
                .get();
        assertEquals(2, masterStream.size());
        assertTrue(masterStream.containsKey(op(3L, "k3")));
        assertTrue(masterStream.containsKey(op(4L, "k4")));
    }

    // =========================================================================
    // 8. GET AND REMOVE CONTAINER VALUE OPERATIONS (write!)
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveContainerValue on master, verify replication to backup")
    void testGetAndRemoveContainerValueOnMaster() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupInitial);

        // getAndRemoveContainerValue on MASTER
        byte[] removedValue = client.setMode(Mode.MASTER)
                .getAndRemoveContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue, StandardCharsets.UTF_8));

        // Verify on MASTER: getSize returns 1
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterSize);

        // Verify on BACKUP: getSize returns 1 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupSize);

        // Verify k1 is gone on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND for removed key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("getAndRemoveContainerValue on backup, verify replication to master")
    void testGetAndRemoveContainerValueOnBackup() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupInitial);

        // getAndRemoveContainerValue on BACKUP
        byte[] removedValue = client.setMode(Mode.BACKUP)
                .getAndRemoveContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue, StandardCharsets.UTF_8));

        // Verify on BACKUP: getSize returns 1
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupSize);

        // Verify on MASTER: getSize returns 1 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterSize);

        // Verify k1 is gone on MASTER
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND for removed key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 9. REMOVE CONTAINER OPERATIONS (write!)
    // =========================================================================

    @Test
    @DisplayName("remove container on master, verify replication to backup")
    void testRemoveContainerOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeContainerMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupInitial);

        // remove container on MASTER
        Boolean removed = client.setMode(Mode.MASTER)
                .remove(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertTrue(removed);

        // Verify on MASTER: container deleted (getSize throws NOT_FOUND)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .getSize(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND for removed container on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP: container deleted (getSize throws NOT_FOUND)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getSize(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND for removed container on backup");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify getContainerValue throws NOT_FOUND on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND for removed container");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("remove container on backup, verify replication to master")
    void testRemoveContainerOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeContainerBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupInitial);

        // remove container on BACKUP
        Boolean removed = client.setMode(Mode.BACKUP)
                .remove(bytes(key), keyHint)
                .get();
        assertTrue(removed);

        // Verify on BACKUP: container deleted (getSize throws NOT_FOUND)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getSize(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND for removed container on backup");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: container deleted (getSize throws NOT_FOUND)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .getSize(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND for removed container on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify getContainerValue throws NOT_FOUND on MASTER
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND for removed container");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 10. GET SIZE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getSize on master, verify replication to backup")
    void testGetSizeOnMaster() throws ExecutionException, InterruptedException {
        String key = "getSizeMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getSize on MASTER
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(bytes(key), keyHint)
                .get();
        assertEquals(3, masterSize);

        // getSize on BACKUP (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(bytes(key), keyHint)
                .get();
        assertEquals(3, backupSize);
    }

    @Test
    @DisplayName("getSize on backup, verify replication to master")
    void testGetSizeOnBackup() throws ExecutionException, InterruptedException {
        String key = "getSizeBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getSize on BACKUP
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(bytes(key), keyHint)
                .get();
        assertEquals(3, backupSize);

        // getSize on MASTER (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(bytes(key), keyHint)
                .get();
        assertEquals(3, masterSize);
    }

    // =========================================================================
    // 11. SET-TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl on master, verify getTtl on both nodes")
    void testSetTtlOnMaster() throws ExecutionException, InterruptedException {
        String key = "setTtlOnMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl on MASTER
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(bytes(key), keyHint, 300000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        // Verify on MASTER: getTtl > 0
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0);

        // Verify on BACKUP: getTtl > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0);
    }

    @Test
    @DisplayName("setTtl on backup, verify getTtl on both nodes")
    void testSetTtlOnBackup() throws ExecutionException, InterruptedException {
        String key = "setTtlOnBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl on BACKUP
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(bytes(key), keyHint, 300000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        // Verify on BACKUP: getTtl > 0
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0);

        // Verify on MASTER: getTtl > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0);
    }

    // =========================================================================
    // 12. TTL EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("TTL expiration: set short TTL on master, verify both nodes expire")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlExpOnMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl = 2 seconds on MASTER
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(bytes(key), keyHint, 2000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: TTL > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0);

        // Verify on BACKUP: TTL > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0);

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on MASTER: getContainerValue throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP: getContainerValue throws NOT_FOUND (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration: set short TTL on backup, verify both nodes expire")
    void testTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttlExpOnBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl = 2 seconds on BACKUP
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(bytes(key), keyHint, 2000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: TTL > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0);

        // Verify on MASTER: TTL > 0 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0);

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on BACKUP: getContainerValue throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: getContainerValue throws NOT_FOUND (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .getContainerValue(bytes(key), keyHint, bytes("k1"))
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 13. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("lockObject on master, verify lock replicates to backup")
    void testLockObjectOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockOnMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on MASTER: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterVal = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterVal);
        assertEquals("v1", new String(masterVal, StandardCharsets.UTF_8));

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID));

        // Unlock by owner
        LockStatus unlockStatus = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject on backup, verify lock replicates to master")
    void testLockObjectOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockOnBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on BACKUP: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupVal = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupVal);
        assertEquals("v1", new String(backupVal, StandardCharsets.UTF_8));

        // Verify on MASTER: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID));

        // Unlock by owner
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    // =========================================================================
    // 14. LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("Lock expiration: set short TTL lock on master, verify both nodes expire")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockExpOnMaster" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK with 2s TTL on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: owner can read
        byte[] masterVal = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterVal);

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID));

        // Wait for lock TTL to expire
        Thread.sleep(3000);

        // Verify on MASTER: lock expired, intruder can read
        byte[] masterValAfter = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterValAfter);

        // Verify on BACKUP: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupValAfter = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupValAfter);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes expire")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockExpOnBackup" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK with 2s TTL on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: owner can read
        byte[] backupVal = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupVal);

        // Verify on MASTER: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID));

        // Wait for lock TTL to expire
        Thread.sleep(3000);

        // Verify on BACKUP: lock expired, intruder can read
        byte[] backupValAfter = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupValAfter);

        // Verify on MASTER: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterValAfter = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterValAfter);
    }

    // =========================================================================
    // 15. INTRUDER CANNOT UNLOCK
    // =========================================================================

    @Test
    @DisplayName("INTRUDER cannot unlock when WRITE_LOCK is held by owner")
    void testIntruderCannotUnlock() throws ExecutionException, InterruptedException {
        String key = "intruderUnlock" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Owner gets WRITE_LOCK on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on MASTER: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] masterVal = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterVal);

        // Intruder cannot unlock on MASTER - gets CANT_UNLOCK
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotEquals(LockStatus.OK, unlock, "Intruder cannot unlock");

        // Verify lock still held on MASTER
        LockStatus stillLocked = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotEquals(LockStatus.OK, stillLocked, "Intruder cannot acquire lock while owner holds it");

        // Owner unlocks
        LockStatus unlockOwner = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockOwner);

        // Verify on BACKUP: lock also released
        Thread.sleep(REPLICATION_DELAY_MS);
        LockStatus unlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockBackup);
    }

    // =========================================================================
    // 16. LOCK EXPIRATION ACCESS
    // =========================================================================

    @Test
    @DisplayName("After lock expiration container is accessible on both nodes")
    void testLockExpirationAccess() throws ExecutionException, InterruptedException {
        String key = "lockExpAccess" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Owner gets WRITE_LOCK with 1s TTL on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID));

        // Wait for lock TTL to expire
        Thread.sleep(2000);

        // Verify on MASTER: lock expired, intruder can read
        byte[] masterValAfter = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterValAfter);
        assertEquals("v1", new String(masterValAfter, StandardCharsets.UTF_8));

        // Verify on BACKUP: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupValAfter = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupValAfter);
        assertEquals("v1", new String(backupValAfter, StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 17. MULTIPLE READ LOCKS
    // =========================================================================

    @Test
    @DisplayName("Two READ_LOCK simultaneously on one container")
    void testMultipleReadLocks() throws ExecutionException, InterruptedException {
        String key = "multiReadLock" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // First client gets READ_LOCK on MASTER
        LockStatus lock1 = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock1);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Second client can also get READ_LOCK on MASTER
        LockStatus lock2 = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.CANT_LOCK, lock2);

        // Both can read on MASTER
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] ownerVal = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(ownerVal);

        byte[] intruderVal = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(intruderVal);

        // Verify on BACKUP: lock is replicated
        Thread.sleep(REPLICATION_DELAY_MS);
        byte[] backupOwnerVal = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupOwnerVal);

        byte[] backupIntruderVal = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupIntruderVal);

        // Both unlock
        LockStatus unlock1 = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock1);
        Thread.sleep(REPLICATION_DELAY_MS);
        LockStatus unlock2 = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock2);
    }

    // =========================================================================
    // 18. READ LOCK EXPIRATION TO WRITE
    // =========================================================================

    @Test
    @DisplayName("After READ_LOCK expiration intruder can get WRITE_LOCK")
    void testReadLockExpirationToWrite() throws ExecutionException, InterruptedException {
        String key = "readLockExpWrite" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Owner gets READ_LOCK with 2s TTL on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: READ_LOCK is replicated
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)));

        // Wait for READ_LOCK to expire
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK on MASTER
        LockStatus intruderLock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, intruderLock);

        // Verify on BACKUP: WRITE_LOCK is replicated
        Thread.sleep(REPLICATION_DELAY_MS);
        LockStatus backupLock = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, backupLock);

        // Unlock
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock);
    }

    // =========================================================================
    // 19. READ LOCK ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("lockObject gets READ_LOCK for ordered map on master - intruder can read but not write")
    void testReadLockOrderedMap() throws ExecutionException, InterruptedException {
        String key = "readLockOrderedMap" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject READ_LOCK on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);

        // Verify on MASTER: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> result = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 0L, Long.MAX_VALUE, false, OWNER_CLIENT_ID)
                .get();
        assertEquals(1, result.size());

        // Verify on BACKUP: intruder CAN read (READ_LOCK allows multiple readers)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 0L, Long.MAX_VALUE, false, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupResult);
        assertEquals(1, backupResult.size());

        // But intruder CANNOT write - addElementOrderedMap should be denied
        assertDenied(client.setMode(Mode.BACKUP)
                .addElementOrderedMap(bytes(key), keyHint,
                        List.of(op(2L, "k2")), List.of(p("v2")), INTRUDER_CLIENT_ID));

        // Unlock
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock);
    }

    // =========================================================================
    // 20. REMOVE ELEMENT AT POSITION END INCLUDED
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition includes endPos in range (tail included)")
    void testRemoveElementAtPositionEndIncluded() throws ExecutionException, InterruptedException {
        String key = "removePosEndIncluded" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, masterInitial);

        Integer backupInitial = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, backupInitial);

        // removeElementAtPosition on MASTER (remove weights 20-30, includes k2@20 and k3@30)
        client.setMode(Mode.MASTER)
                .removeElementAtPosition(bytes(key), keyHint, 20L, 30L)
                .get();

        // Verify on MASTER: getSize returns 2
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Verify on BACKUP: getSize returns 2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        // Verify k1 and k4 remain on BACKUP
        byte[] backupK1 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"))
                .get();
        assertNotNull(backupK1);
        assertEquals("v1", new String(backupK1, StandardCharsets.UTF_8));

        byte[] backupK4 = client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k4"))
                .get();
        assertNotNull(backupK4);
        assertEquals("v4", new String(backupK4, StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 21. REMOVE ELEMENT AT POSITION SAME POS
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition works when boundaries match")
    void testRemoveElementAtPositionSamePos() throws ExecutionException, InterruptedException {
        String key = "removePosSame" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterInitial = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterInitial);

        // removeElementAtPosition on MASTER (remove weight 20 only, minWeight = maxWeight = 20)
        client.setMode(Mode.MASTER)
                .removeElementAtPosition(bytes(key), keyHint, 20L, 20L)
                .get();

        // Verify on MASTER: getSize returns 2
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Verify on BACKUP: getSize returns 2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        // Verify k2 is gone on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .getContainerValue(bytes(key), keyHint, bytes("k2"))
                    .get();
            fail("Expected NOT_FOUND for removed element");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 22. STREAM ELEMENT IN RANGE ORDERED MAP NO MATCH
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap outside range returns empty on both nodes")
    void testStreamElementInRangeOrderedMapNoMatch() throws ExecutionException, InterruptedException {
        String key = "streamNoMatch" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // streamElementInRangeOrderedMap on MASTER (weights 100-200, does not exist)
        Map<OrderedPayload, Payload> streamResult = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 100L, 200L, false, OWNER_CLIENT_ID)
                .get();
        assertNotNull(streamResult);
        assertEquals(0, streamResult.size(), "Should return empty map");

        // Verify on BACKUP: streamElementInRangeOrderedMap also returns empty (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 100L, 200L, false, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStream);
        assertEquals(0, backupStream.size(), "Should return empty map on backup");
    }

    // =========================================================================
    // 23. STREAM ELEMENT IN RANGE ORDERED MAP REVERSE
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap reverse=true returns in reverse order on both nodes")
    void testStreamElementInRangeOrderedMapReverse() throws ExecutionException, InterruptedException {
        String key = "streamReverse" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // streamElementInRangeOrderedMap on MASTER (weights 20-40, reverse)
        Map<OrderedPayload, Payload> streamResult = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 20L, 40L, true, OWNER_CLIENT_ID)
                .get();
        assertNotNull(streamResult);
        assertEquals(3, streamResult.size(), "Should have 3 elements");

        // Verify on BACKUP: streamElementInRangeOrderedMap returns same elements (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 20L, 40L, true, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());

        // Verify all 3 elements exist
        assertTrue(backupStream.containsKey(op(20L, "k2")));
        assertTrue(backupStream.containsKey(op(30L, "k3")));
        assertTrue(backupStream.containsKey(op(40L, "k4")));
    }

    // =========================================================================
    // 24. UNSUPPORTED METHODS FOR ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to ORDERED MAP should throw an error")
    void testUnsupportedMethodsForOrderedMap() throws ExecutionException, InterruptedException {
        String key = "unsupportedOrderedMap" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getElementAtPosition - not applicable to ORDERED MAP
        try {
            client.setMode(Mode.MASTER)
                    .getElementAtPosition(bytes(key), keyHint, 0)
                    .get();
            fail("getElementAtPosition should throw an error for ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getAndRemoveElementAtPosition - not applicable to ORDERED MAP
        try {
            client.setMode(Mode.MASTER)
                    .getAndRemoveElementAtPosition(bytes(key), keyHint, 0)
                    .get();
            fail("getAndRemoveElementAtPosition should throw an error for ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // getHead - not applicable to ORDERED MAP
        try {
            client.setMode(Mode.MASTER)
                    .getHead(bytes(key), keyHint)
                    .get();
            fail("getHead should throw an error for ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail - not applicable to ORDERED MAP
        try {
            client.setMode(Mode.MASTER)
                    .getTail(bytes(key), keyHint)
                    .get();
            fail("getTail should throw an error for ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 25. WRITE LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("After WRITE_LOCK expiration intruder can get lock on both nodes")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String key = "writeLockExp" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Owner gets WRITE_LOCK with 2s TTL on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: WRITE_LOCK is replicated (intruder cannot get lock)
        Thread.sleep(REPLICATION_DELAY_MS);
        LockStatus intruderLockBefore = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotEquals(LockStatus.OK, intruderLockBefore);

        // Wait for WRITE_LOCK to expire
        Thread.sleep(3000);

        // Now intruder can get WRITE_LOCK on MASTER
        LockStatus intruderLock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, intruderLock);

        // Verify on BACKUP: WRITE_LOCK is replicated
        Thread.sleep(REPLICATION_DELAY_MS);
        LockStatus backupLock = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, backupLock);

        // Unlock
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock);
    }

    // =========================================================================
    // 26. WRITE LOCK INTRUDER DENIED
    // =========================================================================

    @Test
    @DisplayName("INTRUDER cannot acquire WRITE_LOCK when WRITE_LOCK exists on both nodes")
    void testWriteLockIntruderDenied() throws ExecutionException, InterruptedException {
        String key = "writeLockDenied" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Owner gets WRITE_LOCK on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: owner can read
        byte[] masterVal = client.setMode(Mode.MASTER)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterVal);

        // Intruder cannot get WRITE_LOCK on MASTER - gets CANT_UNLOCK
        LockStatus intruderLock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotEquals(LockStatus.OK, intruderLock, "Intruder should not acquire lock");

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getContainerValue(bytes(key), keyHint, bytes("k1"), INTRUDER_CLIENT_ID));

        // Unlock by owner
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock);
    }

    // =========================================================================
    // 27. WRITE LOCK ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("lockObject gets WRITE_LOCK for ordered map on both nodes")
    void testWriteLockOrderedMap() throws ExecutionException, InterruptedException {
        String key = "writeLockOrderedMap" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock);

        // Verify on MASTER: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> result = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 0L, Long.MAX_VALUE, false, OWNER_CLIENT_ID)
                .get();
        assertEquals(1, result.size());

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 0L, Long.MAX_VALUE, false, INTRUDER_CLIENT_ID));

        // Unlock
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock);
    }

    // =========================================================================
    // 28. STREAM ORDERED MAP WITH CLIENT ID
    // =========================================================================

    @Test
    @DisplayName("streamOrderedMap with explicit clientId on both nodes")
    void testStreamOrderedMapWithClientId() throws ExecutionException, InterruptedException {
        String key = "streamWithClientId" + UUID.randomUUID();
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        // Create ordered map
        KeyHintData keyHint = client.createOrderedMap(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // streamOrderedMap with explicit clientId on MASTER
        Map<OrderedPayload, Payload> result = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 0L, Long.MAX_VALUE, false, OWNER_CLIENT_ID)
                .get();
        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.containsKey(op(1L, "k1")));
        assertTrue(result.containsKey(op(2L, "k2")));
        assertTrue(result.containsKey(op(3L, "k3")));

        // Verify on BACKUP: streamOrderedMap returns same elements (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Map<OrderedPayload, Payload> backupResult = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedMap(bytes(key), keyHint, 0L, Long.MAX_VALUE, false, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupResult);
        assertEquals(3, backupResult.size());
        assertTrue(backupResult.containsKey(op(1L, "k1")));
        assertTrue(backupResult.containsKey(op(2L, "k2")));
        assertTrue(backupResult.containsKey(op(3L, "k3")));
    }

}