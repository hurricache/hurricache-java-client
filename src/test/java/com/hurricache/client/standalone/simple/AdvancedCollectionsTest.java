package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
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
        Assertions.assertNotNull(client.createList(listKey, List.of(Variant.ofBytes("Middle".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);

        Integer boolResponse = client.addElementToHead(listKey, null, List.of(Variant.ofBytes("Head".getBytes(StandardCharsets.UTF_8)))).get();

        Integer boolResponse1 = client.addElementToPosition(listKey, null, List.of(Variant.ofBytes("NewPos1".getBytes(StandardCharsets.UTF_8))), 1).get();

        Variant head = client.getHead(listKey).get();
        Variant pos1 = client.getElementAtPosition(listKey, 1).get();

        Assertions.assertNotNull(head);
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("Head", new String(head.bytesValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("NewPos1", new String(pos1.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testTailAndPositionalRemoval() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(vecKey, List.of(Variant.ofBytes("0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        client.addElementToTail(vecKey, null, Arrays.asList(
                Variant.ofBytes("1".getBytes(StandardCharsets.UTF_8)),
                Variant.ofBytes("2".getBytes(StandardCharsets.UTF_8)))).get();

        // After createVector + addElementToTail: ["0", "1", "2"]
        // removeTail removes "2": ["0", "1"]
        client.deleteTail(vecKey).get();

        // deleteElementAtPosition(0, 0) removes only position 0 ("0")
        client.deleteElementAtPosition(vecKey, null, 0, 0).get();

        // Only "1" remains
        Variant remaining = client.getElementAtPosition(vecKey, 0).get();
        Assertions.assertNotNull(remaining);
        Assertions.assertEquals("1", new String(remaining.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRemoveElementInRangeSuccess() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(key, List.of(Variant.ofBytes("0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.addElementToTail(key, null, List.of(Variant.ofBytes(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))).get();
        }

        Boolean statusList = client.deleteElementAtPosition(key, null, 0, 2).get();
        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafety() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue" + UUID.randomUUID();
        Assertions.assertNotNull(client.createQueue(qKey, List.of(Variant.ofBytes("q1".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);

        try {
            client.addElementToPosition(qKey, null, List.of(Variant.ofBytes("fail".getBytes(StandardCharsets.UTF_8))), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}