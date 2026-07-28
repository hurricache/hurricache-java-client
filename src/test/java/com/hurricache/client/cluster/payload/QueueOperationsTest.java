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

public class QueueOperationsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testQueueLifecycleCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(first)))
                .get();
        Assertions.assertNotNull(keyHint);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on backup
        boolean added = client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, List.of(Payload.of(second)))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on backup
        Payload headData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.getValue());

        // 4. getAndRemoveFront (Atomic pop from head) on backup
        Payload popped = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.getValue());

        // 5. Verify the new head is the second message on backup
        Payload newHeadData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.getValue());

        // 6. removeHead (Delete without returning data) on backup
        boolean removed = client.setMode(Mode.BACKUP).removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on backup
        Payload empty = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertTrue(empty == null || empty.getValue() == null || empty.getValue().length == 0);
    }

    @Test
    void testQueueLifecycleCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(first)))
                .get();
        Assertions.assertNotNull(keyHint);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on master
        boolean added = client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, List.of(Payload.of(second)))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on master
        Payload headData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.getValue());

        // 4. getAndRemoveFront (Atomic pop from head) on master
        Payload popped = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.getValue());

        // 5. Verify the new head is the second message on master
        Payload newHeadData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.getValue());

        // 6. removeHead (Delete without returning data) on master
        boolean removed = client.setMode(Mode.MASTER).removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on master
        Payload empty = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertTrue(empty == null || empty.getValue() == null || empty.getValue().length == 0);
    }

    @Test
    void testQueueOrderPersistenceCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] initial = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(initial)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, Arrays.asList(Payload.of(first), Payload.of(second)))
                .get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertArrayEquals(initial, client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue());
        Assertions.assertArrayEquals(first, client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue());
        Assertions.assertArrayEquals(second, client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue());
    }

    @Test
    void testQueueOrderPersistenceCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] initial = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(initial)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, Arrays.asList(Payload.of(first), Payload.of(second)))
                .get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertArrayEquals(initial, client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue());
        Assertions.assertArrayEquals(first, client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue());
        Assertions.assertArrayEquals(second, client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue());
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        byte[] qKey = createLargePayload(KEY_SIZE);

        // Test addElementToTail on non-existent key
        try {
            client.addElementToTail(qKey, null, List.of(Payload.of(createLargePayload(VALUE_SIZE)))).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}