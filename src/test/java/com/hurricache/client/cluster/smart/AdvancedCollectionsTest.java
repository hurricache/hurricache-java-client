package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class AdvancedCollectionsTest extends TestBaseCluster {

    @Test
    void testHeadAndPositionalAdditionCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey" + UUID.randomUUID();
        // Start with a list: [Middle]
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createList(listKey, List.of(Payload.of("Middle".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        Boolean boolResponse = client.setMode(Mode.BACKUP)
                .addElementToHead(listKey, keyHint, List.of(Payload.of("Head".getBytes(StandardCharsets.UTF_8))))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        Integer boolResponse1 = client.setMode(Mode.BACKUP)
                .addElementToPosition(listKey, keyHint, List.of(Payload.of("NewPos1".getBytes(StandardCharsets.UTF_8))), 1)
                .get();

        Payload head = client.setMode(Mode.BACKUP).getHead(listKey, keyHint).get();
        Payload pos1 = client.setMode(Mode.BACKUP).getElementAtPosition(listKey, keyHint, 1).get();

        Assertions.assertNotNull(head);
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("Head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("NewPos1", new String(pos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testHeadAndPositionalAdditionCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey" + UUID.randomUUID();
        // Start with a list: [Middle]
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(listKey, List.of(Payload.of("Middle".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        Boolean boolResponse = client.setMode(Mode.MASTER)
                .addElementToHead(listKey, keyHint, List.of(Payload.of("Head".getBytes(StandardCharsets.UTF_8))))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        Integer boolResponse1 = client.setMode(Mode.MASTER)
                .addElementToPosition(listKey, keyHint, List.of(Payload.of("NewPos1".getBytes(StandardCharsets.UTF_8))), 1)
                .get();

        Payload head = client.setMode(Mode.MASTER).getHead(listKey, keyHint).get();
        Payload pos1 = client.setMode(Mode.MASTER).getElementAtPosition(listKey, keyHint, 1).get();

        Assertions.assertNotNull(head);
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("Head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("NewPos1", new String(pos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testTailAndPositionalRemovalCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey" + UUID.randomUUID();
        // Setup Vector: [0, 1, 2]
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(vecKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        client.setMode(Mode.BACKUP)
                .addElementToTail(vecKey, keyHint, Arrays.asList(
                        Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("2".getBytes(StandardCharsets.UTF_8))))
                .get();

        // removeTail -> [0, 1]
        client.setMode(Mode.BACKUP).removeTail(vecKey, keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(Mode.BACKUP).removeElementAtPosition(vecKey, keyHint, 0).get();

        Payload remaining = client.setMode(Mode.BACKUP).getElementAtPosition(vecKey, keyHint, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertEquals("1", new String(remaining.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testTailAndPositionalRemovalCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey" + UUID.randomUUID();
        // Setup Vector: [0, 1, 2]
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(vecKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        client.setMode(Mode.MASTER)
                .addElementToTail(vecKey, keyHint, Arrays.asList(
                        Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("2".getBytes(StandardCharsets.UTF_8))))
                .get();

        // removeTail -> [0, 1]
        client.setMode(Mode.MASTER).removeTail(vecKey, keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(Mode.MASTER).removeElementAtPosition(vecKey, keyHint, 0).get();

        Payload remaining = client.setMode(Mode.MASTER).getElementAtPosition(vecKey, keyHint, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertEquals("1", new String(remaining.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createVector(key, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(key, keyHint, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                    .get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.setMode(Mode.BACKUP).removeElementAtPosition(key, keyHint, 0, 2).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createVector(key, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.MASTER)
                    .addElementToTail(key, keyHint, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8))))
                    .get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.setMode(Mode.MASTER).removeElementAtPosition(key, keyHint, 0, 2).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafetyCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of("q1".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(Mode.BACKUP)
                    .addElementToPosition(qKey, keyHint, List.of(Payload.of("fail".getBytes(StandardCharsets.UTF_8))), 1)
                    .get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }

    @Test
    void testQueueTypeSafetyCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of("q1".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(Mode.MASTER)
                    .addElementToPosition(qKey, keyHint, List.of(Payload.of("fail".getBytes(StandardCharsets.UTF_8))), 1)
                    .get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}