package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CollectionsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testListEdgeOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(listKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.BACKUP).addElementToTail(listKey, keyHint, List.of(Payload.of(one))).get();

        // Get Position
        byte[] posVal = client.setMode(Mode.BACKUP).getElementAtPosition(listKey, keyHint, 1).get().getValue();
        Assertions.assertArrayEquals(one, posVal);

        // Remove Head
        Boolean headRemoved = client.setMode(Mode.BACKUP).removeHead(listKey, keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testListEdgeOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(listKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.MASTER).addElementToTail(listKey, keyHint, List.of(Payload.of(one))).get();

        // Get Position
        byte[] posVal = client.setMode(Mode.MASTER).getElementAtPosition(listKey, keyHint, 1).get().getValue();
        Assertions.assertArrayEquals(one, posVal);

        // Remove Head
        Boolean headRemoved = client.setMode(Mode.MASTER).removeHead(listKey, keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreamingCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(rangeKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.BACKUP).addElementToTail(rangeKey, keyHint, List.of(Payload.of(item))).get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.LIST, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().getValue());
    }

    @Test
    void testRangeStreamingCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(rangeKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.MASTER).addElementToTail(rangeKey, keyHint, List.of(Payload.of(item))).get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.LIST, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().getValue());
    }

    @Test
    void testRangeStreamingVectorCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(rangeKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.BACKUP).addElementToTail(rangeKey, keyHint, List.of(Payload.of(item))).get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.VECTOR, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().getValue());
    }

    @Test
    void testRangeStreamingVectorCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(rangeKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.MASTER).addElementToTail(rangeKey, keyHint, List.of(Payload.of(item))).get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.VECTOR, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().getValue());
    }

    @Test
    void testCreateAndStreamListCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] val1 = createLargePayload(VALUE_SIZE);
        byte[] val2 = createLargePayload(VALUE_SIZE);

        // Create List with first element on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(Payload.of(val1)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on backup
        client.setMode(Mode.BACKUP).addElementToTail(key, keyHint, List.of(Payload.of(val2))).get();

        List<Payload> results = client.setMode(Mode.BACKUP).streamList(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(val1, results.get(0).getValue());
        Assertions.assertArrayEquals(val2, results.get(1).getValue());
    }

    @Test
    void testCreateAndStreamListCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] val1 = createLargePayload(VALUE_SIZE);
        byte[] val2 = createLargePayload(VALUE_SIZE);

        // Create List with first element on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key, List.of(Payload.of(val1)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on master
        client.setMode(Mode.MASTER).addElementToTail(key, keyHint, List.of(Payload.of(val2))).get();

        List<Payload> results = client.setMode(Mode.MASTER).streamList(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(val1, results.get(0).getValue());
        Assertions.assertArrayEquals(val2, results.get(1).getValue());
    }

    @Test
    void testCreateAndStreamVectorCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(key, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.BACKUP).addElementToTail(key, keyHint, List.of(Payload.of(one))).get();

        List<Payload> results = client.setMode(Mode.BACKUP).streamVector(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(zero, results.get(0).getValue());
    }

    @Test
    void testCreateAndStreamVectorCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.MASTER).addElementToTail(key, keyHint, List.of(Payload.of(one))).get();

        List<Payload> results = client.setMode(Mode.MASTER).streamVector(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(zero, results.get(0).getValue());
    }

    @Test
    void testFrontBackOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.BACKUP).addElementToTail(key, keyHint, List.of(Payload.of(one))).get();

        // Get Head/Front
        Payload head = client.setMode(Mode.BACKUP).getHead(key, keyHint).get();
        Payload front = client.setMode(Mode.BACKUP).getFront(key, keyHint).get();
        Assertions.assertArrayEquals(zero, head.getValue());
        Assertions.assertArrayEquals(zero, front.getValue());

        // Get Tail
        Payload tail = client.setMode(Mode.BACKUP).getTail(key, keyHint).get();
        Assertions.assertArrayEquals(one, tail.getValue());
    }

    @Test
    void testFrontBackOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.MASTER).addElementToTail(key, keyHint, List.of(Payload.of(one))).get();

        // Get Head/Front
        Payload head = client.setMode(Mode.MASTER).getHead(key, keyHint).get();
        Payload front = client.setMode(Mode.MASTER).getFront(key, keyHint).get();
        Assertions.assertArrayEquals(zero, head.getValue());
        Assertions.assertArrayEquals(zero, front.getValue());

        // Get Tail
        Payload tail = client.setMode(Mode.MASTER).getTail(key, keyHint).get();
        Assertions.assertArrayEquals(one, tail.getValue());
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] head = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.createList(key, List.of(Payload.of(head))).get();
        byte[] tail = createLargePayload(VALUE_SIZE);
        client.addElementToTail(key, keyHint, List.of(Payload.of(tail))).get();

        // Remove Front
        Payload removedHead = client.getAndRemoveFront(key, keyHint).get();
        Assertions.assertArrayEquals(head, removedHead.getValue());

        // Verify tail is now head
        Payload newHead = client.getFront(key, keyHint).get();
        Assertions.assertArrayEquals(tail, newHead.getValue());
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] init = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.createVector(key, List.of(Payload.of(init))).get();
        byte[] zero = createLargePayload(VALUE_SIZE);
        byte[] one = createLargePayload(VALUE_SIZE);
        client.addElementToTail(key, keyHint,
                                Arrays.asList(Payload.of(zero), Payload.of(one))).get();

        // Get At Position 1
        Payload pos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertArrayEquals(zero, pos1.getValue());

        // Remove At Position 1
        Payload removed = client.getAndRemoveElementAtPosition(key, keyHint, 1).get();
        Assertions.assertArrayEquals(zero, removed.getValue());

        List<Payload> results = client.streamVector(key, keyHint).get();

        System.out.println(results);

        // Verify Shift
        Payload newPos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertArrayEquals(one, newPos1.getValue());
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.createList(key, List.of(Payload.of(zero))).get();
        byte[] one = createLargePayload(VALUE_SIZE);
        byte[] two = createLargePayload(VALUE_SIZE);
        client.addElementToTail(key, keyHint,
                                Arrays.asList(Payload.of(one), Payload.of(two))).get();

        // Get At Position 1
        Payload pos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertArrayEquals(one, pos1.getValue());

        // Remove At Position 1
        Payload removed = client.getAndRemoveElementAtPosition(key, keyHint, 1).get();
        Assertions.assertArrayEquals(one, removed.getValue());

        List<Payload> results = client.streamList(key, keyHint).get();

        System.out.println(results);
        // Verify Shift
        Payload newPos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertArrayEquals(two, newPos1.getValue());
    }

    @Test
    void testCollectionNotFound() {
        byte[] key = createLargePayload(KEY_SIZE);
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