package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CollectionsTest extends TestBase {

    @Test
    void testListEdgeOperations() throws ExecutionException, InterruptedException {
        String listKey = "testVector";
        KeyHintData keyHint = client.createVector(
                listKey,
                List.of(Payload.of("middle".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                listKey,
                keyHint,
                List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8)))
        ).get();

        // Get Position
        Payload posVal = client.getElementAtPosition(listKey, keyHint, 1).get();
        Assertions.assertEquals("tail", new String(posVal.getValue(), StandardCharsets.UTF_8));

        // Remove Head
        Boolean headRemoved = client.removeHead(listKey, keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreaming() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList";
        KeyHintData keyHint = client.createList(
                rangeKey,
                List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))
        ).get();

        for (int i = 1; i < 10; i++) {
            client.addElementToTail(
                    rangeKey,
                    keyHint,
                    List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))
            ).get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.LIST, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size());
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testRangeStreamingVector() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeVector";
        KeyHintData keyHint = client.createVector(
                rangeKey,
                List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))
        ).get();

        for (int i = 1; i < 10; i++) {
            client.addElementToTail(
                    rangeKey,
                    keyHint,
                    List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))
            ).get();
        }

        // Get elements from index 2 to 5
        List<Payload> rangeData = client.streamElementInRangeUnordered(rangeKey, keyHint, ContainerType.VECTOR, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size());
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testCreateAndStreamList() throws ExecutionException, InterruptedException {
        String key = "listTestKey";
        String val1 = "item1";
        String val2 = "item2";

        // Create List with first element
        KeyHintData keyHint = client.createList(
                key,
                List.of(Payload.of(val1.getBytes(StandardCharsets.UTF_8)))
        ).get();

        // Add second element
        client.addElementToTail(
                key,
                keyHint,
                List.of(Payload.of(val2.getBytes(StandardCharsets.UTF_8)))
        ).get();

        List<String> results = client.streamList(key, keyHint).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVector() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey";
        KeyHintData keyHint = client.createVector(
                key,
                List.of(Payload.of("v1".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                key,
                keyHint,
                List.of(Payload.of("v2".getBytes(StandardCharsets.UTF_8)))
        ).get();

        List<String> results = client.streamVector(key, keyHint).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testFrontBackOperations() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey";
        KeyHintData keyHint = client.createList(
                key,
                List.of(Payload.of("head".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                key,
                keyHint,
                List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8)))
        ).get();

        // Get Head/Front
        Payload head = client.getHead(key, keyHint).get();
        Payload front = client.getFront(key, keyHint).get();
        Assertions.assertEquals("head", new String(head.getValue(), StandardCharsets.UTF_8));
        Assertions.assertEquals("head", new String(front.getValue(), StandardCharsets.UTF_8));

        // Get Tail
        Payload tail = client.getTail(key, keyHint).get();
        Assertions.assertEquals("tail", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        String key = "removalTestKey";
        KeyHintData keyHint = client.createList(
                key,
                List.of(Payload.of("item1".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                key,
                keyHint,
                List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8)))
        ).get();

        // Remove Front
        Payload removed = client.getAndRemoveFront(key, keyHint).get();
        Assertions.assertEquals("item1", new String(removed.getValue(), StandardCharsets.UTF_8));

        // Verify tail is now head
        Payload newHead = client.getFront(key, keyHint).get();
        Assertions.assertEquals("item2", new String(newHead.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        String key = "posTestKeyVector";
        KeyHintData keyHint = client.createVector(
                key,
                List.of(Payload.of("pos0".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                key,
                keyHint,
                Arrays.asList(
                        Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("pos2".getBytes(StandardCharsets.UTF_8))
                )
        ).get();

        // Get At Position 1
        Payload pos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        // Remove At Position 1
        Payload removed = client.getAndRemoveElementAtPosition(key, keyHint, 1).get();
        Assertions.assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        CountDownLatch latch = new CountDownLatch(1);
        List<Payload> results = client.streamVector(key, keyHint).get();

        latch.await(5, TimeUnit.SECONDS);
        System.out.println(results);

        // Verify Shift
        Payload newPos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertEquals("pos2", new String(newPos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        String key = "posTestKeyList";
        KeyHintData keyHint = client.createList(
                key,
                List.of(Payload.of("pos0".getBytes(StandardCharsets.UTF_8)))
        ).get();

        client.addElementToTail(
                key,
                keyHint,
                Arrays.asList(
                        Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                        Payload.of("pos2".getBytes(StandardCharsets.UTF_8))
                )
        ).get();

        // Get At Position 1
        Payload pos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        // Remove At Position 1
        Payload removed = client.getAndRemoveElementAtPosition(key, keyHint, 1).get();
        Assertions.assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<Payload> results = client.streamList(key, keyHint).get();

        System.out.println(results);
        // Verify Shift
        Payload newPos1 = client.getElementAtPosition(key, keyHint, 1).get();
        Assertions.assertEquals("pos2", new String(newPos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testCollectionNotFound() {
        String key = "nonExistentCollection";
        try {
            client.getFront(key, null).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Server should return NOT_FOUND if key doesn't exist
            Assertions.assertTrue(cause.getStatus().getCode() == Status.Code.NOT_FOUND
                                  || cause.getStatus().getCode() == Status.Code.INTERNAL);
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}