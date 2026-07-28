package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CollectionsTest extends TestBaseCluster {

    @Test
    void testListEdgeOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String listKey = "testVector" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(listKey, List.of(Payload.of("middle".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        client.setMode(Mode.BACKUP)
                .addElementToTail(listKey, keyHint, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8))))
                .get();

        // Get Position
        Payload posVal = client.setMode(Mode.BACKUP).getElementAtPosition(listKey, keyHint, 1).get();
        Assertions.assertNotNull(posVal);
        Assertions.assertEquals("tail", new String(posVal.getValue(), StandardCharsets.UTF_8));

        // Remove Head
        Boolean headRemoved = client.setMode(Mode.BACKUP).removeHead(listKey, keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testListEdgeOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String listKey = "testVector" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(listKey, List.of(Payload.of("middle".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        client.setMode(Mode.MASTER)
                .addElementToTail(listKey, keyHint, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8))))
                .get();

        // Get Position
        Payload posVal = client.setMode(Mode.MASTER).getElementAtPosition(listKey, keyHint, 1).get();
        Assertions.assertNotNull(posVal);
        Assertions.assertEquals("tail", new String(posVal.getValue(), StandardCharsets.UTF_8));

        // Remove Head
        Boolean headRemoved = client.setMode(Mode.MASTER).removeHead(listKey, keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreamingCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(rangeKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(rangeKey, keyHint, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                    .get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.LIST, 2, 5)
                .get();

        Assertions.assertNotNull(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRangeStreamingCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(rangeKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(Mode.MASTER)
                    .addElementToTail(rangeKey, keyHint, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                    .get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.LIST, 2, 5)
                .get();

        Assertions.assertNotNull(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRangeStreamingVectorCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeVector" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(rangeKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(rangeKey, keyHint, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                    .get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.VECTOR, 2, 5)
                .get();

        Assertions.assertNotNull(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRangeStreamingVectorCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeVector" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(rangeKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(Mode.MASTER)
                    .addElementToTail(rangeKey, keyHint, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                    .get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.VECTOR, 2, 5)
                .get();

        Assertions.assertNotNull(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testCreateAndStreamListCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "listTestKey" + UUID.randomUUID();
        String val1 = "item1";
        String val2 = "item2";

        // Create List with first element on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(Payload.of(val1.getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on backup
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, List.of(Payload.of(val2.getBytes(StandardCharsets.UTF_8))))
                .get();

        List<String> results = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamListCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "listTestKey" + UUID.randomUUID();
        String val1 = "item1";
        String val2 = "item2";

        // Create List with first element on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key, List.of(Payload.of(val1.getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on master
        client.setMode(Mode.MASTER)
                .addElementToTail(key, keyHint, List.of(Payload.of(val2.getBytes(StandardCharsets.UTF_8))))
                .get();

        List<String> results = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVectorCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(key, List.of(Payload.of("v1".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, List.of(Payload.of("v2".getBytes(StandardCharsets.UTF_8))))
                .get();

        List<String> results = client.setMode(Mode.BACKUP)
                .streamVector(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testCreateAndStreamVectorCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(Payload.of("v1".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        client.setMode(Mode.MASTER)
                .addElementToTail(key, keyHint, List.of(Payload.of("v2".getBytes(StandardCharsets.UTF_8))))
                .get();

        List<String> results = client.setMode(Mode.MASTER)
                .streamVector(key, keyHint)
                .get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testFrontBackOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(Payload.of("head".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8))))
                .get();

        // Get Head/Front
        Payload head = client.setMode(Mode.BACKUP).getHead(key, keyHint).get();
        Payload front = client.setMode(Mode.BACKUP).getFront(key, keyHint).get();
        Assertions.assertNotNull(head);
        Assertions.assertNotNull(front);
        Assertions.assertEquals("head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("head", new String(front.getValue(), StandardCharsets.UTF_8));

        // Get Tail
        Payload tail = client.setMode(Mode.BACKUP).getTail(key, keyHint).get();
        Assertions.assertNotNull(tail);
        Assertions.assertEquals("tail", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testFrontBackOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key, List.of(Payload.of("head".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        client.setMode(Mode.MASTER)
                .addElementToTail(key, keyHint, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8))))
                .get();

        // Get Head/Front
        Payload head = client.setMode(Mode.MASTER).getHead(key, keyHint).get();
        Payload front = client.setMode(Mode.MASTER).getFront(key, keyHint).get();
        Assertions.assertNotNull(head);
        Assertions.assertNotNull(front);
        Assertions.assertEquals("head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("head", new String(front.getValue(), StandardCharsets.UTF_8));

        // Get Tail
        Payload tail = client.setMode(Mode.MASTER).getTail(key, keyHint).get();
        Assertions.assertNotNull(tail);
        Assertions.assertEquals("tail", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        String key = "removalTestKey" + UUID.randomUUID();
        KeyHintData keyHint = client.createList(key, List.of(Payload.of("item1".getBytes(StandardCharsets.UTF_8)))).get();
        Thread.sleep(150);
        client.addElementToTail(key, keyHint, List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8)))).get();

        // Remove Front
        Payload removed = client.getAndRemoveFront(key, keyHint).get();
        Assertions.assertNotNull(removed);
        Assertions.assertEquals("item1", new String(removed.getValue(), StandardCharsets.UTF_8));

        // Verify tail is now head
        Payload newHead = client.getFront(key, keyHint).get();
        Assertions.assertNotNull(newHead);
        Assertions.assertEquals("item2", new String(newHead.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        String key = "posTestKeyVector" + UUID.randomUUID();
        KeyHintData keyHint = client.createVector(key, List.of(Payload.of("pos0".getBytes(StandardCharsets.UTF_8)))).get();
        Thread.sleep(150);
        client.addElementToTail(key, keyHint,
                                Arrays.asList(
                                        Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                                        Payload.of("pos2".getBytes(StandardCharsets.UTF_8)))).get();

        // Get At Position 1
        Payload pos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        // Remove At Position 1
        Payload removed = client.getAndRemoveElementAtPosition(key, keyHint, 1).get();
        Assertions.assertNotNull(removed);
        Assertions.assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        CountDownLatch latch = new CountDownLatch(1);
        List<Payload> results = client.streamVector(key, keyHint).get();

        latch.await(5, TimeUnit.SECONDS);

        // Verify Shift
        Payload newPos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertNotNull(newPos1);
        Assertions.assertEquals("pos2", new String(newPos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        String key = "posTestKeyList" + UUID.randomUUID();
        KeyHintData keyHint = client.createList(key, List.of(Payload.of("pos0".getBytes(StandardCharsets.UTF_8)))).get();
        Thread.sleep(150);
        client.addElementToTail(key, keyHint,
                                Arrays.asList(
                                        Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                                        Payload.of("pos2".getBytes(StandardCharsets.UTF_8)))).get();

        // Get At Position 1
        Payload pos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        // Remove At Position 1
        Payload removed = client.getAndRemoveElementAtPosition(key, keyHint, 1).get();
        Assertions.assertNotNull(removed);
        Assertions.assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<Payload> results = client.streamList(key, keyHint).get();

        // Verify Shift
        Payload newPos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertNotNull(newPos1);
        Assertions.assertEquals("pos2", new String(newPos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testCollectionNotFound() {
        String key = "nonExistentCollection" + UUID.randomUUID();
        try {
            client.getFront(key).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Server should return NOT_FOUND if key doesn't exist
            Assertions.assertTrue(cause.getStatus().getCode() == Status.Code.NOT_FOUND
                                  || cause.getStatus().getCode() == Status.Code.INTERNAL);
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}