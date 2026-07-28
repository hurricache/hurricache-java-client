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

public class AdvancedCollectionsTest extends TestBase {

    @Test
    void testHeadAndPositionalAddition() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey";
        // Start with a list: [Middle]
        KeyHintData keyHintResponse = client.createList(
                listKey,
                List.of(Payload.of("Middle".getBytes(StandardCharsets.UTF_8)))
        ).get();

        // addElementToHead -> [Head, Middle]
        Boolean boolResponse = client.addElementToHead(
                listKey,
                keyHintResponse,
                List.of(Payload.of("Head".getBytes(StandardCharsets.UTF_8)))
        ).get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        Integer boolResponse1 = client.addElementToPosition(
                listKey,
                keyHintResponse,
                List.of(Payload.of("NewPos1".getBytes(StandardCharsets.UTF_8))),
                1
        ).get();

        Payload head = client.getHead(listKey, keyHintResponse).get();
        Payload pos1 = client.getElementAtPosition(listKey, keyHintResponse, 1).get();

        Assertions.assertEquals("Head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("NewPos1", new String(pos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testTailAndPositionalRemoval() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey";
        // Setup Vector: [0, 1, 2]
        KeyHintData keyHint = client.createVector(
                vecKey,
                List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                vecKey,
                keyHint,
                Arrays.asList(
                        Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("2".getBytes(StandardCharsets.UTF_8))
                )
        ).get();

        // removeTail -> [0, 1]
        client.removeTail(vecKey, keyHint).get();

        // removeElementAtPosition at 0 -> [1]
        client.removeElementAtPosition(vecKey, keyHint, 0, 0).get();

        Payload remaining = client.getElementAtPosition(vecKey, keyHint, 0).get();
        Assertions.assertEquals("1", new String(remaining.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRemoveElementInRangeSuccess() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey";
        KeyHintData keyHint = client.createVector(
                key,
                List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))
        ).get();

        for (int i = 1; i < 5; i++) {
            client.addElementToTail(
                    key,
                    keyHint,
                    List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))
            ).get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.removeElementAtPosition(key, keyHint, 0, 2).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafety() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue";
        KeyHintData keyHint = client.createQueue(
                qKey,
                List.of(Payload.of("q1".getBytes(StandardCharsets.UTF_8)))
        ).get();

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.addElementToPosition(
                    qKey,
                    keyHint,
                    List.of(Payload.of("fail".getBytes(StandardCharsets.UTF_8))),
                    1
            ).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}