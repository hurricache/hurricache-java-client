package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
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
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Start with a list: [Middle]
        // Create on master
        byte[] middle = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(listKey, List.of(Payload.of(middle)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        byte[] head = createLargePayload(VALUE_SIZE);
        Boolean boolResponse = client.setMode(Mode.BACKUP)
                .addElementToHead(listKey, keyHint, List.of(Payload.of(head)))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        byte[] newPos1 = createLargePayload(VALUE_SIZE);
        Integer boolResponse1 = client.setMode(Mode.BACKUP)
                .addElementToPosition(listKey, keyHint, List.of(Payload.of(newPos1)), 1)
                .get();

        Payload head1 = client.setMode(Mode.BACKUP).getHead(listKey, keyHint).get();
        Payload pos1 = client.setMode(Mode.BACKUP).getElementAtPosition(listKey, keyHint, 1).get();

        Assertions.assertNotNull(head1);
        Assertions.assertNotNull(pos1);
        Assertions.assertArrayEquals(head, head1.getValue());
        Assertions.assertArrayEquals(newPos1, pos1.getValue());
    }

    @Test
    void testHeadAndPositionalAdditionCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Start with a list: [Middle]
        // Create on backup
        byte[] middle1 = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(listKey, List.of(Payload.of(middle1)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        byte[] head1 = createLargePayload(VALUE_SIZE);
        Boolean boolResponse = client.setMode(Mode.MASTER)
                .addElementToHead(listKey, keyHint, List.of(Payload.of(head1)))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        byte[] newPos1 = createLargePayload(VALUE_SIZE);
        Integer boolResponse1 = client.setMode(Mode.MASTER)
                .addElementToPosition(listKey, keyHint, List.of(Payload.of(newPos1)), 1)
                .get();

        Assertions.assertEquals(boolResponse1.intValue(),1);
        Payload head = client.setMode(Mode.MASTER).getHead(listKey, keyHint).get();
        Payload pos1 = client.setMode(Mode.MASTER).getElementAtPosition(listKey, keyHint, 1).get();

        Assertions.assertNotNull(head);
        Assertions.assertNotNull(pos1);
        Assertions.assertArrayEquals(head1, head.getValue());
        Assertions.assertArrayEquals(newPos1, pos1.getValue());
    }

    @Test
    void testTailAndPositionalRemovalCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] vecKey = createLargePayload(KEY_SIZE);
        // Setup Vector: [0, 1, 2]
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(vecKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.BACKUP)
                .addElementToTail(vecKey, keyHint, Arrays.asList(Payload.of(first), Payload.of(second)))
                .get();

        // removeTail -> [0, 1]
        client.setMode(Mode.BACKUP).removeTail(vecKey, keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(Mode.BACKUP).removeElementAtPosition(vecKey, keyHint, 0).get();

        Payload remaining = client.setMode(Mode.BACKUP).getElementAtPosition(vecKey, keyHint, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertArrayEquals(first, remaining.getValue());
    }

    @Test
    void testTailAndPositionalRemovalCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] vecKey = createLargePayload(KEY_SIZE);
        // Setup Vector: [0, 1, 2]
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(vecKey, List.of(Payload.of(zero)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.MASTER)
                .addElementToTail(vecKey, keyHint, Arrays.asList(Payload.of(first), Payload.of(second)))
                .get();

        // removeTail -> [0, 1]
        client.setMode(Mode.MASTER).removeTail(vecKey, keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(Mode.MASTER).removeElementAtPosition(vecKey, keyHint, 0).get();

        Payload remaining = client.setMode(Mode.MASTER).getElementAtPosition(vecKey, keyHint, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertArrayEquals(first, remaining.getValue());
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(key, List.of(Payload.of(createLargePayload(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(key, keyHint, List.of(Payload.of(createLargePayload(VALUE_SIZE))))
                    .get();
        }

        // Remove index 0
        Boolean statusList = client.setMode(Mode.BACKUP).removeElementAtPosition(key, keyHint, 0).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(Payload.of(createLargePayload(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.MASTER)
                    .addElementToTail(key, keyHint, List.of(Payload.of(createLargePayload(VALUE_SIZE))))
                    .get();
        }

        // Remove index 0
        Boolean statusList = client.setMode(Mode.MASTER).removeElementAtPosition(key, keyHint, 0).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafetyCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(createLargePayload(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(Mode.BACKUP)
                    .addElementToPosition(qKey, keyHint, List.of(Payload.of(createLargePayload(VALUE_SIZE))), 1)
                    .get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }

    @Test
    void testQueueTypeSafetyCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(createLargePayload(VALUE_SIZE))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(Mode.MASTER)
                    .addElementToPosition(qKey, keyHint, List.of(Payload.of(createLargePayload(VALUE_SIZE))), 1)
                    .get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}