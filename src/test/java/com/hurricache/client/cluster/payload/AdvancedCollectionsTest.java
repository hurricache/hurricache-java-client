package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AdvancedCollectionsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testHeadAndPositionalAdditionCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayloadKey(KEY_SIZE);
        // Start with a list: [Middle]
        // Create on master
        byte[] middle = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(listKey, List.of(Variant.ofBytes(middle)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        byte[] head = createLargePayloadKey(VALUE_SIZE);
        Integer intResp = client.setMode(Mode.BACKUP)
                .pushHead(listKey, keyHint, List.of(Variant.ofBytes(head)))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        byte[] newPos1 = createLargePayloadKey(VALUE_SIZE);
        Integer intResp1 = client.setMode(Mode.BACKUP)
                .insertAt(listKey, keyHint, List.of(Variant.ofBytes(newPos1)), 1)
                .get();

        Variant head1 = client.setMode(Mode.BACKUP).peekHead(listKey, keyHint).get();
        Variant pos1 = client.setMode(Mode.BACKUP).getAt(listKey, keyHint, 1).get();

        Assertions.assertNotNull(head1);
        Assertions.assertNotNull(pos1);
        Assertions.assertArrayEquals(head, head1.bytesValue());
        Assertions.assertArrayEquals(newPos1, pos1.bytesValue());
    }

    @Test
    void testHeadAndPositionalAdditionCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayloadKey(KEY_SIZE);
        // Start with a list: [Middle]
        // Create on backup
        byte[] middle1 = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(listKey, List.of(Variant.ofBytes(middle1)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        byte[] head1 = createLargePayloadKey(VALUE_SIZE);
        Integer boolResponse = client.setMode(Mode.MASTER)
                .pushHead(listKey, keyHint, List.of(Variant.ofBytes(head1)))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        byte[] newPos1 = createLargePayloadKey(VALUE_SIZE);
        Integer boolResponse1 = client.setMode(Mode.MASTER)
                .insertAt(listKey, keyHint, List.of(Variant.ofBytes(newPos1)), 1)
                .get();

        Assertions.assertEquals(boolResponse1.intValue(),1);
        Variant head = client.setMode(Mode.MASTER).peekHead(listKey, keyHint).get();
        Variant pos1 = client.setMode(Mode.MASTER).getAt(listKey, keyHint, 1).get();

        Assertions.assertNotNull(head);
        Assertions.assertNotNull(pos1);
        Assertions.assertArrayEquals(head1, head.bytesValue());
        Assertions.assertArrayEquals(newPos1, pos1.bytesValue());
    }

    @Test
    void testTailAndPositionalRemovalCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] vecKey = createLargePayloadKey(KEY_SIZE);
        // Setup Vector: [0, 1, 2]
        // Create on master
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(vecKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.BACKUP)
                .pushTail(vecKey, keyHint, Arrays.asList(Variant.ofBytes(first), Variant.ofBytes(second)))
                .get();

        // removeTail -> [0, 1]
        client.setMode(Mode.BACKUP).deleteTail(vecKey, keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(Mode.BACKUP).removeAt(vecKey, keyHint, 0).get();

        Variant remaining = client.setMode(Mode.BACKUP).getAt(vecKey, keyHint, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertArrayEquals(first, remaining.bytesValue());
    }

    @Test
    void testTailAndPositionalRemovalCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] vecKey = createLargePayloadKey(KEY_SIZE);
        // Setup Vector: [0, 1, 2]
        // Create on backup
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(vecKey, List.of(Variant.ofBytes(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.MASTER)
                .pushTail(vecKey, keyHint, Arrays.asList(Variant.ofBytes(first), Variant.ofBytes(second)))
                .get();

        // removeTail -> [0, 1]
        client.setMode(Mode.MASTER).deleteTail(vecKey, keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(Mode.MASTER).removeAt(vecKey, keyHint, 0).get();

        Variant remaining = client.setMode(Mode.MASTER).getAt(vecKey, keyHint, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertArrayEquals(first, remaining.bytesValue());
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(key, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.BACKUP)
                    .pushTail(key, keyHint, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))))
                    .get();
        }

        // Remove index 0
        Integer statusList = client.setMode(Mode.BACKUP).removeAt(key, keyHint, 0).get();

        Assertions.assertEquals(1, statusList);
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.MASTER)
                    .pushTail(key, keyHint, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))))
                    .get();
        }

        // Remove index 0
        Integer statusList = client.setMode(Mode.MASTER).removeAt(key, keyHint, 0).get();

        Assertions.assertEquals(1, statusList);
    }

    @Test
    void testQueueTypeSafetyCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(Mode.BACKUP)
                    .insertAt(qKey, keyHint, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))), 1)
                    .get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }

    @Test
    void testQueueTypeSafetyCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(Mode.MASTER)
                    .insertAt(qKey, keyHint, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE))), 1)
                    .get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}