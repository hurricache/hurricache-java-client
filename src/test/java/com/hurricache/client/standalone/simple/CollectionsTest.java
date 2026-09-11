package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CollectionsTest extends TestBase {

    @Test
    void testListEdgeOperations() throws ExecutionException, InterruptedException {
        String listKey = "testVector" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(listKey, List.of(Payload.of("middle".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);

        client.addElementToTail(listKey, null, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8)))).get();

        Payload posVal = client.getElementAtPosition(listKey, 1).get();
        Assertions.assertNotNull(posVal);
        Assertions.assertEquals("tail", new String(posVal.getValue(), StandardCharsets.UTF_8));

        Boolean headRemoved = client.removeHead(listKey).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreaming() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList" + UUID.randomUUID();
        Assertions.assertNotNull(client.createList(rangeKey, List.of(Payload.of("0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.addElementToTail(rangeKey, null, List.of(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))).get();
        }

        List<Payload> rangeData = client.streamElementInRangeUnordered(rangeKey, null, ContainerType.LIST, 2, 5).get();

        Assertions.assertNotNull(rangeData);
        Assertions.assertEquals(4, rangeData.size());
        Assertions.assertEquals("2", new String(rangeData.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testCreateAndStreamList() throws ExecutionException, InterruptedException {
        String key = "listTestKey" + UUID.randomUUID();
        String val1 = "item1";
        String val2 = "item2";

        Assertions.assertNotNull(client.createList(key, List.of(Payload.of(val1.getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        client.addElementToTail(key, null, List.of(Payload.of(val2.getBytes(StandardCharsets.UTF_8)))).get();

        List<String> results = client.streamList(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVector() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(key, List.of(Payload.of("v1".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        client.addElementToTail(key, null, List.of(Payload.of("v2".getBytes(StandardCharsets.UTF_8)))).get();

        List<String> results = client.streamVector(key).get()
                .stream()
                .map(p -> new String(p.getValue(), StandardCharsets.UTF_8))
                .toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testFrontBackOperations() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createList(key, List.of(Payload.of("head".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(500);
        client.addElementToTail(key, null, List.of(Payload.of("tail".getBytes(StandardCharsets.UTF_8)))).get();

        Payload head = client.getHead(key).get();
        Assertions.assertNotNull(head);
        Assertions.assertEquals("head", new String(head.getValue(), StandardCharsets.UTF_8));

        Payload tail = client.getTail(key).get();
        Assertions.assertNotNull(tail);
        Assertions.assertEquals("tail", new String(tail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        String key = "removalTestKey" + UUID.randomUUID();
        Assertions.assertNotNull(client.createList(key, List.of(Payload.of("item1".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(150);
        client.addElementToTail(key, null, List.of(Payload.of("item2".getBytes(StandardCharsets.UTF_8)))).get();

        Payload removed = client.getAndRemoveFront(key).get();
        Assertions.assertNotNull(removed);
        Assertions.assertEquals("item1", new String(removed.getValue(), StandardCharsets.UTF_8));

        Payload newHead = client.getHead(key).get();
        Assertions.assertNotNull(newHead);
        Assertions.assertEquals("item2", new String(newHead.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        String key = "posTestKeyVector" + UUID.randomUUID();
        Assertions.assertNotNull(client.createVector(key, List.of(Payload.of("pos0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(150);
        client.addElementToTail(key, null, Arrays.asList(
                Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos2".getBytes(StandardCharsets.UTF_8)))).get();

        Payload pos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        Payload removed = client.getAndRemoveElementAtPosition(key, null, 1).get();
        Assertions.assertNotNull(removed);
        Assertions.assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<Payload> results = client.streamVector(key).get();

        Payload newPos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertNotNull(newPos1);
        Assertions.assertEquals("pos2", new String(newPos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        String key = "posTestKeyList" + UUID.randomUUID();
        Assertions.assertNotNull(client.createList(key, List.of(Payload.of("pos0".getBytes(StandardCharsets.UTF_8)))).get());
        Thread.sleep(150);
        client.addElementToTail(key, null, Arrays.asList(
                Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos2".getBytes(StandardCharsets.UTF_8)))).get();

        Payload pos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertNotNull(pos1);
        Assertions.assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        Payload removed = client.getAndRemoveElementAtPosition(key, null, 1).get();
        Assertions.assertNotNull(removed);
        Assertions.assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        List<Payload> results = client.streamList(key).get();

        Payload newPos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertNotNull(newPos1);
        Assertions.assertEquals("pos2", new String(newPos1.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testCollectionNotFound() {
        String key = "nonExistentCollection" + UUID.randomUUID();
        try {
            client.getHead(key).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertTrue(cause.getStatus().getCode() == Status.Code.NOT_FOUND
                                  || cause.getStatus().getCode() == Status.Code.INTERNAL);
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}