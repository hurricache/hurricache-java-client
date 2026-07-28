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

public class QueueOperationsTest extends TestBaseCluster {

    @Test
    void testQueueLifecycleCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue" + UUID.randomUUID();
        String first = "message1";
        String second = "message2";

        // 1. createQueue with initial value on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(first.getBytes(StandardCharsets.UTF_8))))
                .get();
        KeyHintData createRes = keyHint;
        Assertions.assertNotNull(createRes);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on backup
        boolean added = client.setMode(Mode.BACKUP).addElementToTail(
                qKey,
                keyHint,
                List.of(Payload.of(second.getBytes(StandardCharsets.UTF_8)))
        ).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on backup
        Payload headData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertEquals(first, new String(headData.getValue(), StandardCharsets.UTF_8));

        // 4. getAndRemoveFront (Atomic pop from head) on backup
        Payload popped = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals(first, new String(popped.getValue(), StandardCharsets.UTF_8));

        // 5. Verify the new head is the second message on backup
        Payload newHeadData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertEquals(second, new String(newHeadData.getValue(), StandardCharsets.UTF_8));

        // 6. removeHead (Delete without returning data) on backup
        boolean removed = client.setMode(Mode.BACKUP).removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on backup
        Payload empty = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertEquals(0, empty.getValue().length);
    }

    @Test
    void testQueueLifecycleCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue" + UUID.randomUUID();
        String first = "message1";
        String second = "message2";

        // 1. createQueue with initial value on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(first.getBytes(StandardCharsets.UTF_8))))
                .get();
        Assertions.assertNotNull(keyHint);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on master
        boolean added = client.setMode(Mode.MASTER).addElementToTail(
                qKey,
                keyHint,
                List.of(Payload.of(second.getBytes(StandardCharsets.UTF_8)))
        ).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on master
        Payload headData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertEquals(first, new String(headData.getValue(), StandardCharsets.UTF_8));

        // 4. getAndRemoveFront (Atomic pop from head) on master
        Payload popped = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals(first, new String(popped.getValue(), StandardCharsets.UTF_8));

        // 5. Verify the new head is the second message on master
        Payload newHeadData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertEquals(second, new String(newHeadData.getValue(), StandardCharsets.UTF_8));

        // 6. removeHead (Delete without returning data) on master
        boolean removed = client.setMode(Mode.MASTER).removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on master
        Payload empty = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertEquals(0, empty.getValue().length);
    }

    @Test
    void testQueueOrderPersistenceCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        // Create on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of("1".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        client.setMode(Mode.BACKUP).addElementToTail(
                qKey,
                keyHint,
                Arrays.asList(
                        Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("3".getBytes(StandardCharsets.UTF_8))
                )
        ).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("2", new String(client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("3", new String(client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testQueueOrderPersistenceCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        // Create on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of("1".getBytes(StandardCharsets.UTF_8))))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        client.setMode(Mode.MASTER).addElementToTail(
                qKey,
                keyHint,
                Arrays.asList(
                        Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("3".getBytes(StandardCharsets.UTF_8))
                )
        ).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("2", new String(client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("3", new String(client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        String qKey = "missingQueue" + UUID.randomUUID();

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