package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
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

public class RelativePositionOperationsTest extends TestBase {

    @Test
    @DisplayName("addElementToPositionBefore: Create on Master, insert before pivot on Backup")
    void testAddElementToPositionBeforeCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "relativeBeforeMaster" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload item1 = Payload.of("item1".getBytes(StandardCharsets.UTF_8));
        Payload item2 = Payload.of("item2".getBytes(StandardCharsets.UTF_8));

        Assertions.assertNotNull(client.createList(key, List.of(pivot)).get());
        Thread.sleep(500);

        Integer success = client.addElementToPositionBefore(key, List.of(item1, item2), pivot).get();

        Assertions.assertTrue(success == 2, "Insertion before pivot should return true");

        List<String> results = client.streamList(key).get()
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

        Assertions.assertNotNull(client.createVector(key, List.of(head, pivot)).get());
        Thread.sleep(500);

        Integer success = client.addElementToPositionBefore(key, List.of(inserted), pivot).get();

        Assertions.assertTrue(success == 1);

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals("head", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
        Assertions.assertEquals("pivot", results.get(2));
    }

    @Test
    @DisplayName("addElementToPositionAfter: Create on Master, insert after pivot on Backup")
    void testAddElementToPositionAfterCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "relativeAfterMaster" + UUID.randomUUID();
        Payload pivot = Payload.of("pivot".getBytes(StandardCharsets.UTF_8));
        Payload tail = Payload.of("tail".getBytes(StandardCharsets.UTF_8));
        Payload item1 = Payload.of("item1".getBytes(StandardCharsets.UTF_8));
        Payload item2 = Payload.of("item2".getBytes(StandardCharsets.UTF_8));

        Assertions.assertNotNull(client.createList(key, List.of(pivot, tail)).get());
        Thread.sleep(500);

        Integer success = client.addElementToPositionAfter(key, List.of(item1, item2), pivot).get();

        Assertions.assertTrue(success == 2, "Insertion after pivot should return true");

        List<String> results = client.streamList(key).get()
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

        Assertions.assertNotNull(client.createVector(key, List.of(pivot)).get());
        Thread.sleep(500);

        Integer success = client.addElementToPositionAfter(key, List.of(inserted), pivot).get();

        Assertions.assertTrue(success == 1);

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals("pivot", results.get(0));
        Assertions.assertEquals("inserted", results.get(1));
    }

    @Test
    @DisplayName("Non-existent pivot should throw StatusRuntimeException NOT_FOUND without altering collection")
    void testMissingPivotReturnsFalse() throws ExecutionException, InterruptedException {
        String key = "missingPivotKey" + UUID.randomUUID();
        Payload pivot = Payload.of("existing_pivot".getBytes(StandardCharsets.UTF_8));
        Payload missingPivot = Payload.of("non_existent_pivot".getBytes(StandardCharsets.UTF_8));
        Payload item = Payload.of("newItem".getBytes(StandardCharsets.UTF_8));

        Assertions.assertNotNull(client.createList(key, List.of(pivot)).get());
        Thread.sleep(150);

        ExecutionException exBefore = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.addElementToPositionBefore(key, List.of(item), missingPivot).get(),
                "Expected ExecutionException when pivot is missing"
        );
        StatusRuntimeException causeBefore = (StatusRuntimeException) exBefore.getCause();
        Assertions.assertEquals(Status.Code.NOT_FOUND, causeBefore.getStatus().getCode());

        ExecutionException exAfter = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.addElementToPositionAfter(key, List.of(item), missingPivot).get(),
                "Expected ExecutionException when pivot is missing"
        );
        StatusRuntimeException causeAfter = (StatusRuntimeException) exAfter.getCause();
        Assertions.assertEquals(Status.Code.NOT_FOUND, causeAfter.getStatus().getCode());

        List<Payload> current = client.streamList(key).get();
        Assertions.assertEquals(1, current.size());
        Assertions.assertEquals("existing_pivot", new String(current.get(0).getValue(), StandardCharsets.UTF_8));
    }
}