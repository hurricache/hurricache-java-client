package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class QueueOperationsTest extends TestBase {

    @Test
    void testQueueLifecycle() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue";
        String first = "message1";
        String second = "message2";

        // 1. createQueue with initial value
        KeyHintData keyHint = client.createQueue(
                qKey,
                List.of(Payload.of(first.getBytes(StandardCharsets.UTF_8)))
        ).get();
        Assertions.assertNotNull(keyHint);

        // 2. addElementToTail
        boolean added = client.addElementToTail(
                qKey,
                keyHint,
                List.of(Payload.of(second.getBytes(StandardCharsets.UTF_8)))
        ).get();
        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        Payload headData = client.getHead(qKey, keyHint).get();
        Assertions.assertNotNull(headData);
        Assertions.assertEquals(first, new String(headData.getValue(), StandardCharsets.UTF_8));

        // 4. getAndRemoveFront (Atomic pop from head)
        Payload popped = client.getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertNotNull(popped);
        Assertions.assertEquals(first, new String(popped.getValue(), StandardCharsets.UTF_8));

        // 5. Verify the new head is the second message
        Payload newHeadData = client.getHead(qKey, keyHint).get();
        Assertions.assertNotNull(newHeadData);
        Assertions.assertEquals(second, new String(newHeadData.getValue(), StandardCharsets.UTF_8));

        // 6. removeHead (Delete without returning data)
        boolean removed = client.removeHead(qKey, keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Payload empty = client.getHead(qKey, keyHint).get();
        Assertions.assertTrue(empty == null || empty.getValue().length == 0);
    }

    @Test
    void testQueueOrderPersistence() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue";
        KeyHintData keyHint = client.createQueue(
                qKey,
                List.of(Payload.of("1".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                qKey,
                keyHint,
                Arrays.asList(
                        Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("3".getBytes(StandardCharsets.UTF_8))
                )
        ).get();

        // FIFO verification: 1 -> 2 -> 3
        Payload first = client.getAndRemoveFront(qKey, keyHint).get();
        Payload second = client.getAndRemoveFront(qKey, keyHint).get();
        Payload third = client.getAndRemoveFront(qKey, keyHint).get();

        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);
        Assertions.assertNotNull(third);

        Assertions.assertEquals("1", new String(first.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("2", new String(second.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("3", new String(third.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        String qKey = "missingQueue";

        // Test addElementToTail on non-existent key
        try {
            client.addElementToTail(
                    qKey,
                    null,
                    List.of(Payload.of("data".getBytes(StandardCharsets.UTF_8)))
            ).get();
            Assertions.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}