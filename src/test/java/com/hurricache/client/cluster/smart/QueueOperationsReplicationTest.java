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

public class QueueOperationsReplicationTest extends TestBaseCluster {

    @Test
    void testQueueLifecycleCreateOnMasterGetOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue " + UUID.randomUUID();
        String first = "message1 " + UUID.randomUUID();
        String second = "message2 " + UUID.randomUUID();

        // 1. createQueue with initial value
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(first.getBytes(StandardCharsets.UTF_8))))
                .get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, List.of(Payload.of(second.getBytes(StandardCharsets.UTF_8))))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        Payload headData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertNotNull(headData);
        Assertions.assertEquals(first, new String(headData.getValue(), StandardCharsets.UTF_8));

        // 4. getAndRemoveFront (Atomic pop from head)
        Payload popped = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertNotNull(popped);
        Assertions.assertEquals(first, new String(popped.getValue(), StandardCharsets.UTF_8));

        // 5. Verify the new head is the second message
        Payload newHeadData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertNotNull(newHeadData);
        Assertions.assertEquals(second, new String(newHeadData.getValue(), StandardCharsets.UTF_8));

        // 6. removeHead (Delete without returning data)
        boolean removed = client.setMode(Mode.BACKUP).removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        Payload emptyB = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Payload emptyM = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertTrue(emptyM == null || emptyM.getValue() == null || emptyM.getValue().length == 0);
        Assertions.assertTrue(emptyB == null || emptyB.getValue() == null || emptyB.getValue().length == 0);
    }

    @Test
    void testQueueLifecycleCreateOnBackupGetOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue" + UUID.randomUUID();
        String first = "message1" + UUID.randomUUID();
        String second = "message2" + UUID.randomUUID();

        // 1. createQueue with initial value on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(first.getBytes(StandardCharsets.UTF_8))))
                .get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail on master
        boolean added = client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, List.of(Payload.of(second.getBytes(StandardCharsets.UTF_8))))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        Payload headData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertNotNull(headData);
        Assertions.assertEquals(first, new String(headData.getValue(), StandardCharsets.UTF_8));

        // 4. getAndRemoveFront (Atomic pop from head)
        Payload popped = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertNotNull(popped);
        Assertions.assertEquals(first, new String(popped.getValue(), StandardCharsets.UTF_8));

        // 5. Verify the new head is the second message
        Payload newHeadData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertNotNull(newHeadData);
        Assertions.assertEquals(second, new String(newHeadData.getValue(), StandardCharsets.UTF_8));

        // 6. removeHead (Delete without returning data)
        boolean removed = client.setMode(Mode.MASTER).removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        Payload emptyB = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Payload emptyM = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertTrue(emptyM == null || emptyM.getValue() == null || emptyM.getValue().length == 0);
        Assertions.assertTrue(emptyB == null || emptyB.getValue() == null || emptyB.getValue().length == 0);
    }

    @Test
    void testQueueOrderPersistenceCMGB() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of("1".getBytes(StandardCharsets.UTF_8))))
                .get();
        Thread.sleep(500);
        client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, Arrays.asList(
                        Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("3".getBytes(StandardCharsets.UTF_8))))
                .get();
        Thread.sleep(500);

        // FIFO verification: 1 -> 2 -> 3
        Payload first = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals("1", new String(first.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(500);
        Payload second = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals("2", new String(second.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(500);
        Payload third = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals("3", new String(third.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testQueueOrderPersistenceCBGM() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of("1".getBytes(StandardCharsets.UTF_8))))
                .get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, Arrays.asList(
                        Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("3".getBytes(StandardCharsets.UTF_8))))
                .get();
        Thread.sleep(500);

        // FIFO verification: 1 -> 2 -> 3
        Payload first = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals("1", new String(first.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(500);
        Payload second = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals("2", new String(second.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(500);
        Payload third = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals("3", new String(third.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        String qKey = "missingQueue";

        // Test addElementToTail on non-existent key
        try {
            client.addElementToTail(qKey, null, List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}