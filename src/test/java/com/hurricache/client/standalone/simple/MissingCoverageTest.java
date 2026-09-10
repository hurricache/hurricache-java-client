package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class MissingCoverageTest extends TestBase {

    private byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    // =========================================================================
    // ATOMIC LOAD OPERATIONS (2 methods)
    // =========================================================================

    @Test
    void atomicLoadTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicLoadKey" + UUID.randomUUID();

        client.atomicCreate(testKey, 42L).get();
        Thread.sleep(150);

        long value = client.atomicLoad(testKey).get();
        Assertions.assertEquals(42L, value);
    }

    @Test
    void atomicLoadOnNonExistKeyTest() {
        String testKey = "atomicLoadNonExistKey" + UUID.randomUUID();

        try {
            client.atomicLoad(testKey).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (ExecutionException e) {
            Assertions.assertEquals(io.grpc.Status.Code.NOT_FOUND,
                ((io.grpc.StatusRuntimeException) e.getCause()).getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void atomicLoadAndDeleteTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicLoadDeleteKey" + UUID.randomUUID();

        client.atomicCreate(testKey, 100L).get();
        Thread.sleep(150);

        long loadedValue = client.atomicLoadAndDelete(testKey).get();
        Assertions.assertEquals(100L, loadedValue);

        Thread.sleep(150);
        try {
            client.atomicLoad(testKey).get();
            Assertions.fail("Expected NOT_FOUND after delete");
        } catch (ExecutionException e) {
            Assertions.assertEquals(io.grpc.Status.Code.NOT_FOUND,
                ((io.grpc.StatusRuntimeException) e.getCause()).getStatus().getCode());
        }
    }

    // =========================================================================
    // ADD ELEMENT ORDERED (1 method)
    // =========================================================================

    @Test
    void addElementOrderedTest() throws ExecutionException, InterruptedException {
        String testKey = "addElementOrderedKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.createOrderedSet(testKey, List.of()).get();
        Thread.sleep(150);

        OrderedPayload op1 = OrderedPayload.of(100L, bytes("first"));
        OrderedPayload op2 = OrderedPayload.of(200L, bytes("second"));

        Integer added = client.addElementOrdered(keyBytes, null, List.of(op1, op2), 0, null).get();

        Assertions.assertTrue(added > 0);
        System.out.println("added: " + added);

        Thread.sleep(150);
        Integer size = client.getSize(testKey).get();
        Assertions.assertEquals(2, size);
    }

    // =========================================================================
    // REMOVE FROM CONTAINER WITH CONTAINERTYPE (1 method)
    // =========================================================================

    @Test
    void removeFromContainerWithTypeTest() throws ExecutionException, InterruptedException {
        String setKey = "removeFromContainerTypeKey" + UUID.randomUUID();

        client.createSet(setKey, List.of(
            Payload.of("item1".getBytes(StandardCharsets.UTF_8)),
            Payload.of("item2".getBytes(StandardCharsets.UTF_8)),
            Payload.of("item3".getBytes(StandardCharsets.UTF_8))
        )).get();
        Thread.sleep(150);

        Integer sizeBefore = client.getSize(setKey).get();
        Assertions.assertEquals(3, sizeBefore);

        Integer removed = client.removeFromContainer(
            setKey.getBytes(StandardCharsets.UTF_8),null,
            "item1".getBytes(StandardCharsets.UTF_8)
        ).get();
        Assertions.assertTrue(removed > 0);

        Thread.sleep(150);
        Integer sizeAfter = client.getSize(setKey).get();
        Assertions.assertEquals(2, sizeAfter);
    }

    // =========================================================================
    // SHUTDOWN (1 method)
    // =========================================================================

    @Test
    void shutdownTest() throws ExecutionException, InterruptedException {
        String testKey = "shutdownTestKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        client.atomicCreate(keyBytes, 99L).get();
        Thread.sleep(150);

        long value = client.atomicLoad(testKey).get();
        Assertions.assertEquals(99L, value);

        client.shutdown();
    }
}