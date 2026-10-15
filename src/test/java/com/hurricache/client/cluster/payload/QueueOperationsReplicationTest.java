package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
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
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);

        // 1. createQueue with initial value
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Variant.ofBytes(first)))
                .get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, List.of(Variant.ofBytes(second)))
                .get();

        Assertions.assertTrue(added == 1);

        // 3. getHead (Peek without removing)
        Variant headData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.bytesValue());

        // 4. getAndRemoveFront (Atomic pop from head)
        Variant popped = client.setMode(Mode.BACKUP).getAndDeleteHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.bytesValue());

        // 5. Verify the new head is the second message
        Variant newHeadData = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.bytesValue());

        // 6. removeHead (Delete without returning data)
        Integer removed = client.setMode(Mode.BACKUP).deleteHead(qKey, keyHint).get();
        Assertions.assertEquals(1, removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        Variant emptyB = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Variant emptyM = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertTrue(emptyM == null || emptyM.bytesValue() == null || emptyM.bytesValue().length == 0);
        Assertions.assertTrue(emptyB == null || emptyB.bytesValue() == null || emptyB.bytesValue().length == 0);
    }

    @Test
    void testQueueLifecycleCreateOnBackupGetOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);

        // 1. createQueue with initial value on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Variant.ofBytes(first)))
                .get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        Integer added = client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, List.of(Variant.ofBytes(second)))
                .get();

        Assertions.assertTrue(added == 1);

        // 3. getHead (Peek without removing)
        Variant headData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.bytesValue());

        // 4. getAndRemoveFront (Atomic pop from head)
        Variant popped = client.setMode(Mode.MASTER).getAndDeleteHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.bytesValue());

        // 5. Verify the new head is the second message
        Variant newHeadData = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.bytesValue());

        // 6. removeHead (Delete without returning data)
        Integer removed = client.setMode(Mode.MASTER).deleteHead(qKey, keyHint).get();
        Assertions.assertEquals(1, removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        Variant emptyB = client.setMode(Mode.BACKUP).getHead(qKey, keyHint).get();
        Variant emptyM = client.setMode(Mode.MASTER).getHead(qKey, keyHint).get();
        Assertions.assertTrue(emptyM == null || emptyM.bytesValue() == null || emptyM.bytesValue().length == 0);
        Assertions.assertTrue(emptyB == null || emptyB.bytesValue() == null || emptyB.bytesValue().length == 0);
    }

    @Test
    void testQueueOrderPersistenceCMGB() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Variant.ofBytes(zero)))
                .get();
        Thread.sleep(500);
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        byte[] two = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.BACKUP)
                .addElementToTail(qKey, keyHint, Arrays.asList(Variant.ofBytes(one), Variant.ofBytes(two)))
                .get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        byte[] three = createLargePayloadKey(VALUE_SIZE);
        client.addElementToTail(qKey, keyHint, List.of(Variant.ofBytes(three))).get();

        Assertions.assertArrayEquals(zero, client.setMode(Mode.MASTER).getAndDeleteHead(qKey, keyHint).get().bytesValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(one, client.setMode(Mode.BACKUP).getAndDeleteHead(qKey, keyHint).get().bytesValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(two, client.setMode(Mode.MASTER).getAndDeleteHead(qKey, keyHint).get().bytesValue());
    }

    @Test
    void testQueueOrderPersistenceCBGM() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        byte[] zero = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Variant.ofBytes(zero)))
                .get();
        Thread.sleep(500);
        byte[] one = createLargePayloadKey(VALUE_SIZE);
        byte[] two = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.MASTER)
                .addElementToTail(qKey, keyHint, Arrays.asList(Variant.ofBytes(one), Variant.ofBytes(two)))
                .get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        byte[] three = createLargePayloadKey(VALUE_SIZE);
        client.addElementToTail(qKey, keyHint, List.of(Variant.ofBytes(three))).get();

        Assertions.assertArrayEquals(zero, client.setMode(Mode.BACKUP).getAndDeleteHead(qKey, keyHint).get().bytesValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(one, client.setMode(Mode.MASTER).getAndDeleteHead(qKey, keyHint).get().bytesValue());
        Thread.sleep(500);
        Assertions.assertArrayEquals(two, client.setMode(Mode.BACKUP).getAndDeleteHead(qKey, keyHint).get().bytesValue());
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);

        // Test addElementToTail on non-existent key
        try {
            client.addElementToTail(qKey, null, List.of(Variant.ofBytes(createLargePayloadKey(VALUE_SIZE)))).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}