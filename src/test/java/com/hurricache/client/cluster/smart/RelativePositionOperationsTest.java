package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class RelativePositionOperationsTest extends TestBaseCluster {

    // ------------------------------------------------------------------------------------------------
    // addElementToPositionBefore Tests
    // ------------------------------------------------------------------------------------------------

    @Test
    @DisplayName("addElementToPositionBefore: Create on Master, insert before pivot on Backup")
    void testAddElementToPositionBeforeCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "relativeBeforeMaster" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload item1 = Payload.of("item1".getBytes(StandardCharsets.UTF_8));
        Payload item2 = Payload.of("item2".getBytes(StandardCharsets.UTF_8));

        // Create initial collection on Master containing the pivot
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(pivot))
                .get();

        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Insert items before pivot via Backup
        Boolean success = client.setMode(Mode.BACKUP)
                .addElementToPositionBefore(key, keyHint, List.of(item1, item2), pivot)
                .get();

        Assertions.assertTrue(success, "Insertion before pivot should return true");

        // Validate the resulting order on Backup: [item1, item2, pivot]
        List<String> results = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("item1", results.get(0));
        Assertions.assertEquals("item2", results.get(1));
        Assertions.assertEquals("pivot", results.get(2));
    }

    @Test
    @DisplayName("addElementToPositionBefore: Create on Backup, insert before pivot on Master")
    void testAddElementToPositionBeforeCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "relativeBeforeBackup" + UUID.randomUUID();
        Payload head = Payload.of("head".getBytes(StandardCharsets.UTF_8));
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload inserted = Payload.of("inserted".getBytes(StandardCharsets.UTF_8));

        // Create initial collection on Backup: [head, pivot]
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(head, pivot))
                .get();

        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Insert item before pivot via Master
        Boolean success = client.setMode(Mode.MASTER)
                .addElementToPositionBefore(key, keyHint, List.of(inserted), pivot)
                .get();

        Assertions.assertTrue(success);

        // Validate on Master: [head, inserted, pivot]
        List<String> results = client.setMode(Mode.MASTER)
                .streamVector(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("head", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
        Assertions.assertEquals("pivot", results.get(2));
    }

    // ------------------------------------------------------------------------------------------------
    // addElementToPositionAfter Tests
    // ------------------------------------------------------------------------------------------------

    @Test
    @DisplayName("addElementToPositionAfter: Create on Master, insert after pivot on Backup")
    void testAddElementToPositionAfterCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "relativeAfterMaster" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload tail = Payload.of("tail".getBytes(StandardCharsets.UTF_8));
        Payload item1 = Payload.of("item1".getBytes(StandardCharsets.UTF_8));
        Payload item2 = Payload.of("item2".getBytes(StandardCharsets.UTF_8));

        // Create initial collection on Master: [pivot, tail]
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(pivot, tail))
                .get();

        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Insert items after pivot via Backup
        Boolean success = client.setMode(Mode.BACKUP)
                .addElementToPositionAfter(key, keyHint, List.of(item1, item2), pivot)
                .get();

        Assertions.assertTrue(success, "Insertion after pivot should return true");

        // Validate the resulting order on Backup: [pivot, item1, item2, tail]
        List<String> results = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(4, results.size());
        Assertions.assertEquals("pivot", results.get(0));
        Assertions.assertEquals("item1", results.get(1));
        Assertions.assertEquals("item2", results.get(2));
        Assertions.assertEquals("tail", results.get(3));
    }

    @Test
    @DisplayName("addElementToPositionAfter: Create on Backup, insert after pivot on Master")
    void testAddElementToPositionAfterCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "relativeAfterBackup" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload inserted = Payload.of("inserted".getBytes(StandardCharsets.UTF_8));

        // Create initial collection on Backup containing the pivot
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(pivot))
                .get();

        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Insert item after pivot via Master
        Boolean success = client.setMode(Mode.MASTER)
                .addElementToPositionAfter(key, keyHint, List.of(inserted), pivot)
                .get();

        Assertions.assertTrue(success);

        // Validate on Master: [pivot, inserted]
        List<String> results = client.setMode(Mode.MASTER)
                .streamVector(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals("pivot", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
    }

    // ------------------------------------------------------------------------------------------------
    // Negative Scenarios
    // ------------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Non-existent pivot should throw StatusRuntimeException NOT_FOUND without altering collection")
    void testMissingPivotReturnsFalse() throws ExecutionException, InterruptedException {
        String key = "missingPivotKey" + UUID.randomUUID();
        Payload pivot = Payload.of("existing_pivot".getBytes(StandardCharsets.UTF_8));
        Payload missingPivot = Payload.of("non_existent_pivot".getBytes(StandardCharsets.UTF_8));
        Payload item = Payload.of("newItem".getBytes(StandardCharsets.UTF_8));

        KeyHintData keyHint = client.createList(key, List.of(pivot)).get();
        Thread.sleep(150);

        // Attempt BEFORE with non-existent pivot
        ExecutionException exBefore = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.addElementToPositionBefore(key, keyHint, List.of(item), missingPivot).get(),
                "Expected ExecutionException when pivot is missing"
        );
        StatusRuntimeException causeBefore = (StatusRuntimeException) exBefore.getCause();
        Assertions.assertEquals(Status.Code.NOT_FOUND, causeBefore.getStatus().getCode());
        Assertions.assertTrue(causeBefore.getStatus().getDescription().contains("Pivot element not found"));

        // Attempt AFTER with non-existent pivot
        ExecutionException exAfter = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.addElementToPositionAfter(key, keyHint, List.of(item), missingPivot).get(),
                "Expected ExecutionException when pivot is missing"
        );
        StatusRuntimeException causeAfter = (StatusRuntimeException) exAfter.getCause();
        Assertions.assertEquals(Status.Code.NOT_FOUND, causeAfter.getStatus().getCode());
        Assertions.assertTrue(causeAfter.getStatus().getDescription().contains("Pivot element not found"));

        // Verify collection size remains unchanged
        List<Payload> current = client.streamList(key, keyHint).get();
        Assertions.assertEquals(1, current.size());
        Assertions.assertEquals("existing_pivot", new String(current.get(0).getValue(), StandardCharsets.UTF_8));
    }
}