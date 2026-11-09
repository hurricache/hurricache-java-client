package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CAS по полю контейнера (HASHMAP / ORDERED_MAP) — compareAndSetInContainer.
 */
public class CasInContainerTest extends TestBase {

    private String newKey() {
        return "cas_container_" + UUID.randomUUID();
    }

    private void createMap(String key) throws Exception {
        Map<com.hurricache.client.intf.Payload, Variant> data = new HashMap<>();
        data.put(com.hurricache.client.intf.Payload.of("counter".getBytes()), Variant.ofLong(100));
        data.put(com.hurricache.client.intf.Payload.of("score".getBytes()), Variant.ofDouble(1.5));
        client.createMap(key, data).get();
    }

    @Test
    @DisplayName("CAS INT field in HashMap: success and failure")
    void testCasIntFieldHashMap() throws Exception {
        String key = newKey();
        createMap(key);

        AtomicCasRes ok = client.compareAndSetInContainer(key, "counter".getBytes(), 100, 200).get();
        assertTrue(ok.getResult());
        assertEquals(200, client.getInContainer(key, "counter".getBytes()).get().longValue());

        AtomicCasRes fail = client.compareAndSetInContainer(key, "counter".getBytes(), 100, 300).get();
        assertFalse(fail.getResult());
        assertEquals(200, client.getInContainer(key, "counter".getBytes()).get().longValue());
    }

    @Test
    @DisplayName("CAS DOUBLE field in HashMap")
    void testCasDoubleFieldHashMap() throws Exception {
        String key = newKey();
        createMap(key);

        AtomicCasRes ok = client.compareAndSetInContainer(key, "score".getBytes(), 1.5, 2.5).get();
        assertTrue(ok.getResult());
        assertEquals(2.5, client.getInContainer(key, "score".getBytes()).get().doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("CAS on OrderedMap element with exact key+weight")
    void testCasOrderedMapExact() throws Exception {
        String key = newKey();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 10L), Variant.ofLong(7));
        client.createOrderedMap(key, data).get();

        AtomicCasRes ok = client.compareAndSetInContainer(key, OrderedPayload.of(10L, "k1".getBytes()), 7, 8).get();
        assertTrue(ok.getResult());

        Variant v = client.getInContainer(key, "k1".getBytes()).get();
        assertEquals(8, v.longValue());

        // неверный вес — элемент не найден
        assertThrows(ExecutionException.class,
                     () -> client.compareAndSetInContainer(key, OrderedPayload.of(999L, "k1".getBytes()), 8, 9).get());
    }

    @Test
    @DisplayName("CAS on missing container key -> error")
    void testCasMissingField() throws Exception {
        String key = newKey();
        createMap(key);

        assertThrows(ExecutionException.class,
                     () -> client.compareAndSetInContainer(key, "missing".getBytes(), 1, 2).get());
    }

    @Test
    @DisplayName("CAS type mismatch -> INVALID_ARGUMENT")
    void testCasTypeMismatch() throws Exception {
        String key = newKey();
        createMap(key);

        // поле counter — INT, expected как DOUBLE
        assertThrows(ExecutionException.class,
                     () -> client.compareAndSetInContainer(key, "counter".getBytes(), 1.5, 2.5).get());
    }
}
