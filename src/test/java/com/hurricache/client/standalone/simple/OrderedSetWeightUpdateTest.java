package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Тесты обновления веса элемента OrderedSet через gRPC:
 * update(..., new_order) — прямая установка, incrementOrder(..., delta) — инкремент/декремент.
 * Поиск элемента — точное совпадение (значение + текущий вес).
 */
public class OrderedSetWeightUpdateTest extends TestBase {

    private String newKey() {
        return "zset_weight_" + UUID.randomUUID();
    }

    private void createSample(String key) throws Exception {
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("alpha".getBytes(), 10L),
                OrderedVariant.ofBytes("beta".getBytes(), 20L),
                OrderedVariant.ofBytes("gamma".getBytes(), 30L)
        );
        client.createOrderedSet(key, data).get();
    }

    private long weightOf(String key, String member) throws Exception {
        for (OrderedVariant ov : client.getOrderedSet(key).get()) {
            if (new String(ov.bytesValue()).equals(member)) {
                return ov.getOrder();
            }
        }
        fail("member not found: " + member);
        return -1;
    }

    @Test
    @DisplayName("incrementOrder: +delta moves element weight")
    void testIncrementOrder() throws Exception {
        String key = newKey();
        createSample(key);

        Boolean ok = client.incrementOrder(key, null, OrderedPayload.of(10L, "alpha".getBytes()), 5, 0, java.time.Duration.ofSeconds(10)).get();
        assertTrue(ok);
        assertEquals(15L, weightOf(key, "alpha"));

        // декремент
        ok = client.incrementOrder(key, null, OrderedPayload.of(15L, "alpha".getBytes()), -15, 0, java.time.Duration.ofSeconds(10)).get();
        assertTrue(ok);
        assertEquals(0L, weightOf(key, "alpha"));
    }

    @Test
    @DisplayName("update: set absolute weight")
    void testSetAbsoluteWeight() throws Exception {
        String key = newKey();
        createSample(key);

        Boolean ok = client.update(key, null, OrderedPayload.of(20L, "beta".getBytes()), 99, 0, java.time.Duration.ofSeconds(10)).get();
        assertTrue(ok);
        assertEquals(99L, weightOf(key, "beta"));
    }

    @Test
    @DisplayName("Exact match: same member with two weights is legal, update touches only the addressed one")
    void testSameMemberTwoWeights() throws Exception {
        String key = newKey();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("dup".getBytes(), 1L),
                OrderedVariant.ofBytes("dup".getBytes(), 2L)
        );
        client.createOrderedSet(key, data).get();

        Boolean ok = client.incrementOrder(key, null, OrderedPayload.of(2L, "dup".getBytes()), 10, 0, java.time.Duration.ofSeconds(10)).get();
        assertTrue(ok);

        List<OrderedVariant> all = client.getOrderedSet(key).get();
        assertEquals(2, all.size());
        assertEquals(1L, all.get(0).getOrder());
        assertEquals(12L, all.get(1).getOrder());
    }

    @Test
    @DisplayName("NOT_FOUND when weight does not match")
    void testWrongCurrentWeight() throws Exception {
        String key = newKey();
        createSample(key);

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, null, OrderedPayload.of(999L, "alpha".getBytes()), 1, 0, java.time.Duration.ofSeconds(10)).get());
    }

    @Test
    @DisplayName("OUT_OF_RANGE on underflow below zero")
    void testUnderflow() throws Exception {
        String key = newKey();
        createSample(key);

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, null, OrderedPayload.of(10L, "alpha".getBytes()), -11, 0, java.time.Duration.ofSeconds(10)).get());

        // значение не изменилось
        assertEquals(10L, weightOf(key, "alpha"));
    }

    @Test
    @DisplayName("elementKey without order is rejected client-side")
    void testMissingOrderRejected() {
        String key = newKey();
        assertThrows(IllegalArgumentException.class,
                     () -> client.incrementOrder(key, null, new OrderedPayload("alpha".getBytes()), 1, 0, java.time.Duration.ofSeconds(10)));
    }

    @Test
    @DisplayName("Wrong container type -> error")
    void testWrongContainerType() throws Exception {
        String key = newKey();
        client.create(key, com.hurricache.client.intf.Variant.ofLong(1)).get();

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, null, OrderedPayload.of(1L, "alpha".getBytes()), 1, 0, java.time.Duration.ofSeconds(10)).get());
    }
}
