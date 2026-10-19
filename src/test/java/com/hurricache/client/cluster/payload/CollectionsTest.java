package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CollectionsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testListEdgeOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayloadKey(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(listKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.BACKUP).pushTail(listKey, keyHint, List.of(Variant.ofBytes(one))).get();

        // Get Position
        byte[] posVal = client.setMode(Mode.BACKUP).getAt(listKey, keyHint, 1).get().bytesValue();
        Assertions.assertArrayEquals(one, posVal);

        // Remove Head
        Integer headRemoved = client.setMode(Mode.BACKUP).deleteHead(listKey, keyHint).get();
        Assertions.assertEquals(1, (int) headRemoved);
    }

    @Test
    void testListEdgeOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(listKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.MASTER).pushTail(listKey, keyHint, List.of(Variant.ofBytes(one))).get();

        // Get Position
        byte[] posVal = client.setMode(Mode.MASTER).getAt(listKey, keyHint, 1).get().bytesValue();
        Assertions.assertArrayEquals(one, posVal);

        // Remove Head
        Integer headRemoved = client.setMode(Mode.MASTER).deleteHead(listKey, keyHint).get();
        Assertions.assertEquals(1, (int) headRemoved);
    }

    @Test
    void testRangeStreamingCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayloadKey(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(rangeKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayloadKey(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.BACKUP).pushTail(rangeKey, keyHint, List.of(Variant.ofBytes(item))).get();
        }

        // Get elements from index 2 to 5
        List<Variant> rangeData = client.setMode(Mode.BACKUP)
                .getRange(rangeKey, keyHint, ContainerType.LIST, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().bytesValue());
    }

    @Test
    void testRangeStreamingCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(rangeKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayloadKey(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.MASTER).pushTail(rangeKey, keyHint, List.of(Variant.ofBytes(item))).get();
        }

        // Get elements from index 2 to 5
        List<Variant> rangeData = client.setMode(Mode.MASTER)
                .getRange(rangeKey, keyHint, ContainerType.LIST, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().bytesValue());
    }

    @Test
    void testRangeStreamingVectorCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayloadKey(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(rangeKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayloadKey(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.BACKUP).pushTail(rangeKey, keyHint, List.of(Variant.ofBytes(item))).get();
        }

        // Get elements from index 2 to 5
        List<Variant> rangeData = client.setMode(Mode.BACKUP)
                .getRange(rangeKey, keyHint, ContainerType.VECTOR, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().bytesValue());
    }

    @Test
    void testRangeStreamingVectorCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(rangeKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayloadKey(VALUE_SIZE);
            if (i == 2) item3 = item;
            client.setMode(Mode.MASTER).pushTail(rangeKey, keyHint, List.of(Variant.ofBytes(item))).get();
        }

        // Get elements from index 2 to 5
        List<Variant> rangeData = client.setMode(Mode.MASTER)
                .getRange(rangeKey, keyHint, ContainerType.VECTOR, 2, 5)
                .get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4
        Assertions.assertArrayEquals(item3, rangeData.iterator().next().bytesValue());
    }

    @Test
    void testCreateAndStreamListCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        byte[] val1 = createLargePayloadKey(VALUE_SIZE);
        byte[] val2 = createLargePayloadKey(VALUE_SIZE);

        // Create List with first element on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(Variant.ofBytes(val1)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on backup
        client.setMode(Mode.BACKUP).pushTail(key, keyHint, List.of(Variant.ofBytes(val2))).get();

        List<Variant> results = client.setMode(Mode.BACKUP).getList(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(val1, results.get(0).bytesValue());
        Assertions.assertArrayEquals(val2, results.get(1).bytesValue());
    }

    @Test
    void testCreateAndStreamListCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        byte[] val1 = createLargePayloadKey(VALUE_SIZE);
        byte[] val2 = createLargePayloadKey(VALUE_SIZE);

        // Create List with first element on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key, List.of(Variant.ofBytes(val1)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on master
        client.setMode(Mode.MASTER).pushTail(key, keyHint, List.of(Variant.ofBytes(val2))).get();

        List<Variant> results = client.setMode(Mode.MASTER).getList(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(val1, results.get(0).bytesValue());
        Assertions.assertArrayEquals(val2, results.get(1).bytesValue());
    }

    @Test
    void testCreateAndStreamVectorCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(key, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.BACKUP).pushTail(key, keyHint, List.of(Variant.ofBytes(one))).get();

        List<Variant> results = client.setMode(Mode.BACKUP).getVector(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(zero, results.get(0).bytesValue());
    }

    @Test
    void testCreateAndStreamVectorCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.MASTER).pushTail(key, keyHint, List.of(Variant.ofBytes(one))).get();

        List<Variant> results = client.setMode(Mode.MASTER).getVector(key, keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(zero, results.get(0).bytesValue());
    }

    @Test
    void testFrontBackOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(key, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.BACKUP).pushTail(key, keyHint, List.of(Variant.ofBytes(one))).get();

        // Get Head/Front
        Variant head = client.setMode(Mode.BACKUP).peekHead(key, keyHint).get();
        Assertions.assertArrayEquals(zero, head.bytesValue());

        // Get Tail
        Variant tail = client.setMode(Mode.BACKUP).peekTail(key, keyHint).get();
        Assertions.assertArrayEquals(one, tail.bytesValue());
    }

    @Test
    void testFrontBackOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.MASTER).pushTail(key, keyHint, List.of(Variant.ofBytes(one))).get();

        // Get Head/Front
        Variant head = client.setMode(Mode.MASTER).peekHead(key, keyHint).get();
        Assertions.assertArrayEquals(zero, head.bytesValue());

        // Get Tail
        Variant tail = client.setMode(Mode.MASTER).peekTail(key, keyHint).get();
        Assertions.assertArrayEquals(one, tail.bytesValue());
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        byte[] head = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.createList(key, List.of(Variant.ofBytes(head))).get();
        byte[] tail = createLargePayloadKey(VALUE_SIZE);
        client.pushTail(key, keyHint, List.of(Variant.ofBytes(tail))).get();

        // Remove Front
        Variant removedHead = client.popHead(key, keyHint).get();
        Assertions.assertArrayEquals(head, removedHead.bytesValue());

        // Verify tail is now head
        Variant newHead = client.peekHead(key, keyHint).get();
        Assertions.assertArrayEquals(tail, newHead.bytesValue());
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        byte[] init = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.createVector(key, List.of(Variant.ofBytes(init))).get();
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        client.pushTail(key, keyHint,
                        Arrays.asList(Variant.ofBytes(zero), Variant.ofBytes(one))).get();

        // Get At Position 1
        Variant pos1 = client.getAt(key, keyHint, 1).get();
        Assertions.assertArrayEquals(zero, pos1.bytesValue());

        // Remove At Position 1
        Variant removed = client.popAt(key, keyHint, 1).get();
        Assertions.assertArrayEquals(zero, removed.bytesValue());

        List<Variant> results = client.getVector(key, keyHint).get();

        System.out.println(results);

        // Verify Shift
        Variant newPos1 = client.getAt(key, keyHint, 1).get();
        Assertions.assertArrayEquals(one, newPos1.bytesValue());
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.createList(key, List.of(Variant.ofBytes(zero))).get();
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        byte[] two = createLargePayloadKey(VALUE_SIZE);
        client.pushTail(key, keyHint,
                        Arrays.asList(Variant.ofBytes(one), Variant.ofBytes(two))).get();

        // Get At Position 1
        Variant pos1 = client.getAt(key, keyHint, 1).get();
        Assertions.assertArrayEquals(one, pos1.bytesValue());

        // Remove At Position 1
        Variant removed = client.popAt(key, keyHint, 1).get();
        Assertions.assertArrayEquals(one, removed.bytesValue());

        List<Variant> results = client.getList(key, keyHint).get();

        System.out.println(results);
        // Verify Shift
        Variant newPos1 = client.getAt(key, keyHint, 1).get();
        Assertions.assertArrayEquals(two, newPos1.bytesValue());
    }

    @Test
    void testCollectionNotFound() {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        try {
            client.peekHead(key, null, 0, Duration.ofSeconds(1)).get();
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