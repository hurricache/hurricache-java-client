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

public class QueueOperationsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testQueueLifecycleCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);

        // 1. createQueue with initial value on master
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Variant.ofBytes(first)))
                .get();
        Assertions.assertNotNull(keyHint);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on backup
        Integer added = client.setMode(Mode.BACKUP)
                .pushTail(qKey, keyHint, List.of(Variant.ofBytes(second)))
                .get();

        Assertions.assertTrue(added == 1);

        // 3. getHead (Peek without removing) on backup
        Variant headData = client.setMode(Mode.BACKUP).peekHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.bytesValue());

        // 4. getAndRemoveFront (Atomic pop from head) on backup
        Variant popped = client.setMode(Mode.BACKUP).popHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.bytesValue());

        // 5. Verify the new head is the second message on backup
        Variant newHeadData = client.setMode(Mode.BACKUP).peekHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.bytesValue());

        // 6. removeHead (Delete without returning data) on backup
        Integer removed = client.setMode(Mode.BACKUP).deleteHead(qKey, keyHint).get();
        Assertions.assertEquals(1, removed);

        // 7. Verify Queue is now empty or key doesn't exist on backup
        Variant empty = client.setMode(Mode.BACKUP).peekHead(qKey, keyHint).get();
        Assertions.assertTrue(empty == null || empty.bytesValue() == null || empty.bytesValue().length == 0);
    }

    @Test
    void testQueueLifecycleCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);

        // 1. createQueue with initial value on backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Variant.ofBytes(first)))
                .get();
        Assertions.assertNotNull(keyHint);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on master
        Integer added = client.setMode(Mode.MASTER)
                .pushTail(qKey, keyHint, List.of(Variant.ofBytes(second)))
                .get();

        Assertions.assertTrue(added == 1);

        // 3. getHead (Peek without removing) on master
        Variant headData = client.setMode(Mode.MASTER).peekHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, headData.bytesValue());

        // 4. getAndRemoveFront (Atomic pop from head) on master
        Variant popped = client.setMode(Mode.MASTER).popHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped.bytesValue());

        // 5. Verify the new head is the second message on master
        Variant newHeadData = client.setMode(Mode.MASTER).peekHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData.bytesValue());

        // 6. removeHead (Delete without returning data) on master
        Integer removed = client.setMode(Mode.MASTER).deleteHead(qKey, keyHint).get();
        Assertions.assertEquals(1, removed);

        // 7. Verify Queue is now empty or key doesn't exist on master
        Variant empty = client.setMode(Mode.MASTER).peekHead(qKey, keyHint).get();
        Assertions.assertTrue(empty == null || empty.bytesValue() == null || empty.bytesValue().length == 0);
    }

    @Test
    void testQueueOrderPersistenceCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        // Create on master
        byte[] initial = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createQueue(qKey, List.of(Variant.ofBytes(initial)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.BACKUP)
                .pushTail(qKey, keyHint, Arrays.asList(Variant.ofBytes(first), Variant.ofBytes(second)))
                .get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertArrayEquals(initial, client.setMode(Mode.BACKUP).popHead(qKey, keyHint).get().bytesValue());
        Assertions.assertArrayEquals(first, client.setMode(Mode.BACKUP).popHead(qKey, keyHint).get().bytesValue());
        Assertions.assertArrayEquals(second, client.setMode(Mode.BACKUP).popHead(qKey, keyHint).get().bytesValue());
    }

    @Test
    void testQueueOrderPersistenceCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);
        // Create on backup
        byte[] initial = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createQueue(qKey, List.of(Variant.ofBytes(initial)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] first = createLargePayloadKey(VALUE_SIZE);
        byte[] second = createLargePayloadKey(VALUE_SIZE);
        client.setMode(Mode.MASTER)
                .pushTail(qKey, keyHint, Arrays.asList(Variant.ofBytes(first), Variant.ofBytes(second)))
                .get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertArrayEquals(initial, client.setMode(Mode.MASTER).popHead(qKey, keyHint).get().bytesValue());
        Assertions.assertArrayEquals(first, client.setMode(Mode.MASTER).popHead(qKey, keyHint).get().bytesValue());
        Assertions.assertArrayEquals(second, client.setMode(Mode.MASTER).popHead(qKey, keyHint).get().bytesValue());
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        byte[] qKey = createLargePayloadKey(KEY_SIZE);

        // Test addElementToTail on non-existent key
        try {
            client.pushTail(qKey, null, List.of(createLargePayload(VALUE_SIZE))).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}