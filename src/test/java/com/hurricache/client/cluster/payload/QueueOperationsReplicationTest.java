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

public class QueueOperationsReplicationTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testQueueLifecycleCreateOnMasterGetOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(first)))
                .get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, List.of(Payload.of(second)))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        Payload headData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.getValue());

        // 4. getAndRemoveFront (Atomic pop from head)
        Payload popped = client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.getValue());

        // 5. Verify the new head is the second message
        Payload newHeadData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.getValue());

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
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(first)))
                .get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, List.of(Payload.of(second)))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        Payload headData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.getValue());

        // 4. getAndRemoveFront (Atomic pop from head)
        Payload popped = client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.getValue());

        // 5. Verify the new head is the second message
        Payload newHeadData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.getValue());

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
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Payload.of(zero)))
                .get();
        Thread.sleep(500);
        byte[] one = createLargePayload(VALUE_SIZE);
        byte[] two = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, Arrays.asList(Payload.of(one), Payload.of(two)))
                .get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        byte[] three = createLargePayload(VALUE_SIZE);
        client.addElementToTail(qKey, keyHint, List.of(Payload.of(three))).get();

        Assertions.assertArrayEquals(zero, client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(one, client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(two, client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue());
    }

    @Test
    void testQueueOrderPersistenceCBGM() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Payload.of(zero)))
                .get();
        Thread.sleep(500);
        byte[] one = createLargePayload(VALUE_SIZE);
        byte[] two = createLargePayload(VALUE_SIZE);
        client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, Arrays.asList(Payload.of(one), Payload.of(two)))
                .get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        byte[] three = createLargePayload(VALUE_SIZE);
        client.addElementToTail(qKey, keyHint, List.of(Payload.of(three))).get();

        Assertions.assertArrayEquals(zero, client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(one, client.setMode(Mode.MASTER).getAndRemoveFront(qKey, keyHint).get().getValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(two, client.setMode(Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get().getValue());
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