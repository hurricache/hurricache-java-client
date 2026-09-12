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

public class AdvancedCollectionsTest extends TestBase {

    @Test
    void testHeadAndPositionalAddition() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createList(listKey, List.of(Payload.of("Middle".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);

        Integer boolResponse = client.addElementToHead(listKey, null, List.of(Payload.of("Head".getBytes(StandardCharsets.UTF_8)))).get();

        Integer boolResponse1 = client.addElementToPosition(listKey, null, List.of(Payload.of("NewPos1".getBytes(StandardCharsets.UTF_8))), 1).get();

        Payload head = client.getHead(listKey).get();
        Payload pos1 = client.getElementAtPosition(listKey, 1).get();

        Assertions.assertNotNull(head);
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("Head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("NewPos1", new String(pos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testTailAndPositionalRemoval() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(vecKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        client.addElementToTail(vecKey, null, Arrays.asList(
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8)))).get();

        // After createVector + addElementToTail: ["0", "1", "2"]
        // removeTail removes "2": ["0", "1"]
        client.removeTail(vecKey).get();

        // removeElementAtPosition(0, 0) removes only position 0 ("0")
        client.removeElementAtPosition(vecKey, null, 0, 0).get();

        // Only "1" remains
        Payload remaining = client.getElementAtPosition(vecKey, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertEquals("1", new String(remaining.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRemoveElementInRangeSuccess() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(key, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.addElementToTail(key, null, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))).get();
        }

        Boolean statusList = client.removeElementAtPosition(key, null, 0, 2).get();
        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafety() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue" + UUID.randomUUID();
        Assertions.assertNotNull(client.createQueue(qKey, List.of(Payload.of("q1".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);

        try {
            client.addElementToPosition(qKey, null, List.of(Payload.of("fail".getBytes(StandardCharsets.UTF_8))), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}