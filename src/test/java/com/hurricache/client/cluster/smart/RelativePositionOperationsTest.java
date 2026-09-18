package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cluster tests for pivot-based insertion operations (addElementToPositionBefore/After).
 * Each test verifies replication between MASTER and BACKUP nodes.
 */
public class RelativePositionOperationsTest extends TestBaseCluster {

    private static final long REPLICATION_DELAY_MS = 300;

    // =========================================================================
    // Section 1: addElementToPositionBefore
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore: write on Master, verify replication to Backup (List)")
    void testAddElementToPositionBeforeListMaster() throws ExecutionException, InterruptedException {
        String key = "relative_before_list_master_" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload item1 = Payload.of("item1".getBytes(StandardCharsets.UTF_8));
        Payload item2 = Payload.of("item2".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, List.of(pivot)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Write on MASTER (insert before pivot)
        Integer added = client.setMode(Mode.MASTER)
                .addElementToPositionBefore(bytes(key), hint, List.of(item1, item2), pivot).get();
        assertEquals(2, added);

        // Verify on MASTER (immediate)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(3, masterStream.size());
        assertEquals("item1", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("item2", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(masterStream.get(2).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(bytes(key), hint).get();
        assertEquals(3, backupStream.size());
        assertEquals("item1", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("item2", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(backupStream.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionBefore: write on Backup, verify replication to Master (List)")
    void testAddElementToPositionBeforeListBackup() throws ExecutionException, InterruptedException {
        String key = "relative_before_list_backup_" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload inserted = Payload.of("inserted".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, List.of(pivot)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Write on BACKUP (insert before pivot)
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionBefore(bytes(key), hint, List.of(inserted), pivot).get();
        assertEquals(1, added);

        // Verify on BACKUP (immediate)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(bytes(key), hint).get();
        assertEquals(2, backupStream.size());
        assertEquals("inserted", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(2, masterStream.size());
        assertEquals("inserted", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionBefore: write on Master, verify replication to Backup (Vector)")
    void testAddElementToPositionBeforeVectorMaster() throws ExecutionException, InterruptedException {
        String key = "relative_before_vector_master_" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload before1 = Payload.of("before1".getBytes(StandardCharsets.UTF_8));
        Payload before2 = Payload.of("before2".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createVector(key, List.of(pivot)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Write on MASTER (insert before pivot)
        Integer added = client.setMode(Mode.MASTER)
                .addElementToPositionBefore(bytes(key), hint, List.of(before1, before2), pivot).get();
        assertEquals(2, added);

        // Verify on MASTER (immediate)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamVector(bytes(key), hint).get();
        assertEquals(3, masterStream.size());
        assertEquals("before1", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("before2", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(masterStream.get(2).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamVector(bytes(key), hint).get();
        assertEquals(3, backupStream.size());
        assertEquals("before1", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("before2", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(backupStream.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionBefore: write on Backup, verify replication to Master (Vector)")
    void testAddElementToPositionBeforeVectorBackup() throws ExecutionException, InterruptedException {
        String key = "relative_before_vector_backup_" + UUID.randomUUID();
        Payload head = Payload.of("head".getBytes(StandardCharsets.UTF_8));
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload inserted = Payload.of("inserted".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createVector(key, List.of(head, pivot)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Write on BACKUP (insert before pivot)
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionBefore(bytes(key), hint, List.of(inserted), pivot).get();
        assertEquals(1, added);

        // Verify on BACKUP (immediate)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamVector(bytes(key), hint).get();
        assertEquals(3, backupStream.size());
        assertEquals("head", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("inserted", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(backupStream.get(2).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamVector(bytes(key), hint).get();
        assertEquals(3, masterStream.size());
        assertEquals("head", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("inserted", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("pivot", new String(masterStream.get(2).getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // Section 2: addElementToPositionAfter
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionAfter: write on Master, verify replication to Backup (List)")
    void testAddElementToPositionAfterListMaster() throws ExecutionException, InterruptedException {
        String key = "relative_after_list_master_" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload tail = Payload.of("tail".getBytes(StandardCharsets.UTF_8));
        Payload item1 = Payload.of("item1".getBytes(StandardCharsets.UTF_8));
        Payload item2 = Payload.of("item2".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, List.of(pivot, tail)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Write on MASTER (insert after pivot)
        Integer added = client.setMode(Mode.MASTER)
                .addElementToPositionAfter(bytes(key), hint, List.of(item1, item2), pivot).get();
        assertEquals(2, added);

        // Verify on MASTER (immediate)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(4, masterStream.size());
        assertEquals("pivot", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("item1", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("item2", new String(masterStream.get(2).getValue(), StandardCharsets.UTF_8));
        assertEquals("tail", new String(masterStream.get(3).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(bytes(key), hint).get();
        assertEquals(4, backupStream.size());
        assertEquals("pivot", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("item1", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));
        assertEquals("item2", new String(backupStream.get(2).getValue(), StandardCharsets.UTF_8));
        assertEquals("tail", new String(backupStream.get(3).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionAfter: write on Backup, verify replication to Master (List)")
    void testAddElementToPositionAfterListBackup() throws ExecutionException, InterruptedException {
        String key = "relative_after_list_backup_" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload inserted = Payload.of("inserted".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, List.of(pivot)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Write on BACKUP (insert after pivot)
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionAfter(bytes(key), hint, List.of(inserted), pivot).get();
        assertEquals(1, added);

        // Verify on BACKUP (immediate)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(bytes(key), hint).get();
        assertEquals(2, backupStream.size());
        assertEquals("pivot", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("inserted", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify MASTER
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(2, masterStream.size());
        assertEquals("pivot", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("inserted", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // Section 3: Missing pivot error
    // =========================================================================

    @Test
    @DisplayName("Non-existent pivot should throw NOT_FOUND without altering collection")
    void testMissingPivotReturnsNotFound() throws ExecutionException, InterruptedException {
        String key = "missing_pivot_" + UUID.randomUUID();
        Payload existingPivot = Payload.of("existing_pivot".getBytes(StandardCharsets.UTF_8));
        Payload missingPivot = Payload.of("non_existent_pivot".getBytes(StandardCharsets.UTF_8));
        Payload newItem = Payload.of("newItem".getBytes(StandardCharsets.UTF_8));

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, List.of(existingPivot)).get();
        assertNotNull(hint);

        // Replication wait
        Thread.sleep(500);

        // Verify list has 1 element before
        List<Payload> beforeStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(1, beforeStream.size());

        // Try addElementToPositionBefore with missing pivot → should throw NOT_FOUND on MASTER
        ExecutionException exBefore = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .addElementToPositionBefore(bytes(key), hint, List.of(newItem), missingPivot).get()
        );
        StatusRuntimeException causeBefore = (StatusRuntimeException) exBefore.getCause();
        assertEquals(Status.Code.NOT_FOUND, causeBefore.getStatus().getCode());

        // Try addElementToPositionAfter with missing pivot → should throw NOT_FOUND on MASTER
        ExecutionException exAfter = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .addElementToPositionAfter(bytes(key), hint, List.of(newItem), missingPivot).get()
        );
        StatusRuntimeException causeAfter = (StatusRuntimeException) exAfter.getCause();
        assertEquals(Status.Code.NOT_FOUND, causeAfter.getStatus().getCode());

        // Verify list unchanged on MASTER
        List<Payload> afterStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(1, afterStream.size());
        assertEquals("existing_pivot", new String(afterStream.get(0).getValue(), StandardCharsets.UTF_8));

        // Replication delay → verify list unchanged on BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(bytes(key), hint).get();
        assertEquals(1, backupStream.size());
        assertEquals("existing_pivot", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
    }
}
