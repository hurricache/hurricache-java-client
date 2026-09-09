package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
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

public class QueueOperationsTest extends TestBase {

    @Test
    void testQueueLifecycle() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue" + UUID.randomUUID();
        String first = "message1";
        String second = "message2";

        client.createQueue(qKey, List.of(Payload.of(first.getBytes(StandardCharsets.UTF_8)))).get();
        Thread.sleep(500);

        Integer added = client.addElementToTail(qKey, null, List.of(Payload.of(second.getBytes(StandardCharsets.UTF_8)))).get();
        Assertions.assertTrue(added == 1);

        Payload headData = client.getHead(qKey).get();
        Assertions.assertEquals(first, new String(headData.getValue(), StandardCharsets.UTF_8));

        Payload popped = client.getAndRemoveFront(qKey).get();
        Assertions.assertEquals(first, new String(popped.getValue(), StandardCharsets.UTF_8));

        Payload newHeadData = client.getHead(qKey).get();
        Assertions.assertEquals(second, new String(newHeadData.getValue(), StandardCharsets.UTF_8));

        boolean removed = client.removeHead(qKey).get();
        Assertions.assertTrue(removed);

        Payload empty = client.getHead(qKey).get();
        Assertions.assertEquals(0, empty.getValue().length);
    }

    @Test
    void testQueueOrderPersistence() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        client.createQueue(qKey, List.of(Payload.of("1".getBytes(StandardCharsets.UTF_8)))).get();
        Thread.sleep(500);

        client.addElementToTail(qKey, null, Arrays.asList(
                Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("3".getBytes(StandardCharsets.UTF_8)))).get();

        Assertions.assertEquals("1", new String(client.getAndRemoveFront(qKey).get().getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("2", new String(client.getAndRemoveFront(qKey).get().getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("3", new String(client.getAndRemoveFront(qKey).get().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        String qKey = "missingQueue" + UUID.randomUUID();
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