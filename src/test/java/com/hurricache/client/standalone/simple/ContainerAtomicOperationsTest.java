/*
 * HurriCache Java Client - Container Atomic Operations Tests
 * Copyright (C) 2024 Sber Bank GCR
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */
package com.hurricache.client.standalone.simple;

import com.hurricache.TestStandAlone;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for container atomic operations (addInContainer, subInContainer, exchangeInContainer,
 * andInContainer, orInContainer, xorInContainer) with FastCacheAsyncSimpleClient.
 * 
 * Covers MAP (by containerKey) and VECTOR/LIST (by position) addressing modes.
 */
public class ContainerAtomicOperationsTest extends TestStandAlone {

    // =========================================================================
    // 1. ADD IN CONTAINER
    // =========================================================================

    @Test
    @DisplayName("addInContainer with MAP by containerKey - long delta")
    void testAddInContainerMapByKey() throws Exception {
        String key = "add_map_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("f1"), Variant.ofLong(10));
        initialData.put(p("f2"), Variant.ofLong(20));
        client.createMap(key, initialData).get();

        // addInContainer: f1 += 5 → returns OLD value 10
        Number oldVal = client.addInContainer(key, p("f1").getValue(), 5).get();
        assertEquals(10, oldVal.longValue());

        // Verify f1 is now 15
        Variant current = client.getInContainer(key, p("f1").getValue()).get();
        assertEquals(15, current.longValue());
    }

    @Test
    @DisplayName("addInContainer with MAP by containerKey - double delta")
    void testAddInContainerMapByDoubleDelta() throws Exception {
        String key = "add_map_double_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("f1"), Variant.ofDouble(10.5));
        client.createMap(key, initialData).get();

        // addInContainer: f1 += 2.5 → returns OLD value 10.5
        Number oldVal = client.addInContainer(key, p("f1").getValue(), 2.5).get();
        assertEquals(10.5, oldVal.doubleValue(), 0.001);

        // Verify f1 is now 13.0
        Variant current = client.getInContainer(key, p("f1").getValue()).get();
        assertEquals(13.0, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("addInContainer with VECTOR by position - long delta")
    void testAddInContainerVectorByPos() throws Exception {
        String key = "add_vector_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(100), Variant.ofLong(200), Variant.ofLong(300));
        client.createVector(key, initialData).get();

        // addInContainer: VECTOR[1] += 50 → returns OLD value 200
        Number oldVal = client.addInContainer(key, 1, 50).get();
        assertEquals(200, oldVal.longValue());

        // Verify VECTOR[1] is now 250
        Variant current = client.getAt(key, 1).get();
        assertEquals(250, current.longValue());
    }

    @Test
    @DisplayName("addInContainer with LIST by position - long delta")
    void testAddInContainerListByPos() throws Exception {
        String key = "add_list_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(10), Variant.ofLong(20), Variant.ofLong(30));
        client.createList(key, initialData).get();

        // addInContainer: LIST[0] += -5 → returns OLD value 10
        Number oldVal = client.addInContainer(key, 0, -5).get();
        assertEquals(10, oldVal.longValue());

        // Verify LIST[0] is now 5
        Variant current = client.getAt(key, 0).get();
        assertEquals(5, current.longValue());
    }

    @Test
    @DisplayName("addInContainer with negative delta")
    void testAddInContainerNegativeDelta() throws Exception {
        String key = "add_map_negative_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("counter"), Variant.ofLong(100));
        client.createMap(key, initialData).get();

        // addInContainer: counter += -30 → returns OLD value 100
        Number oldVal = client.addInContainer(key, p("counter").getValue(), -30).get();
        assertEquals(100, oldVal.longValue());

        // Verify counter is now 70
        Variant current = client.getInContainer(key, p("counter").getValue()).get();
        assertEquals(70, current.longValue());
    }

    @Test
    @DisplayName("addInContainer on non-existent MAP field - NOT_FOUND")
    void testAddInContainerNonExistentKey() throws Exception {
        String key = "add_map_nofield_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("f1"), Variant.ofLong(10));
        client.createMap(key, initialData).get();

        // addInContainer on non-existent field → should throw NOT_FOUND
        try {
            client.addInContainer(key, p("nonexistent").getValue(), 5).get();
            fail("Expected NOT_FOUND exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("addInContainer on non-existent VECTOR position - OUT_OF_RANGE")
    void testAddInContainerNonExistentPos() throws Exception {
        String key = "add_vector_nopos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(10), Variant.ofLong(20), Variant.ofLong(30));
        client.createVector(key, initialData).get();

        // addInContainer on non-existent position → should throw OUT_OF_RANGE
        try {
            client.addInContainer(key, 999, 5).get();
            fail("Expected OUT_OF_RANGE exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.OUT_OF_RANGE, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 2. SUB IN CONTAINER
    // =========================================================================

    @Test
    @DisplayName("subInContainer with MAP by containerKey")
    void testSubInContainerMapByKey() throws Exception {
        String key = "sub_map_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(100));
        client.createMap(key, initialData).get();

        // subInContainer: val -= 25 → returns OLD value 100
        Number oldVal = client.subInContainer(key, p("val").getValue(), 25).get();
        assertEquals(100, oldVal.longValue());

        // Verify val is now 75
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(75, current.longValue());
    }

    @Test
    @DisplayName("subInContainer with VECTOR by position")
    void testSubInContainerVectorByPos() throws Exception {
        String key = "sub_vector_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(50), Variant.ofLong(100), Variant.ofLong(150));
        client.createVector(key, initialData).get();

        // subInContainer: VECTOR[1] -= 30 → returns OLD value 100
        Number oldVal = client.subInContainer(key, 1, 30).get();
        assertEquals(100, oldVal.longValue());

        // Verify VECTOR[1] is now 70
        Variant current = client.getAt(key, 1).get();
        assertEquals(70, current.longValue());
    }

    @Test
    @DisplayName("subInContainer goes negative")
    void testSubInContainerGoesNegative() throws Exception {
        String key = "sub_map_negative_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(10));
        client.createMap(key, initialData).get();

        // subInContainer: val -= 30 → returns OLD value 10
        Number oldVal = client.subInContainer(key, p("val").getValue(), 30).get();
        assertEquals(10, oldVal.longValue());

        // Verify val is now -20
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(-20, current.longValue());
    }

    // =========================================================================
    // 3. EXCHANGE IN CONTAINER
    // =========================================================================

    @Test
    @DisplayName("exchangeInContainer with MAP by containerKey")
    void testExchangeInContainerMap() throws Exception {
        String key = "exch_map_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(50));
        client.createMap(key, initialData).get();

        // exchangeInContainer: val = 99 → returns OLD value 50
        Number oldVal = client.exchangeInContainer(key, p("val").getValue(), 99).get();
        assertEquals(50, oldVal.longValue());

        // Verify val is now 99
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(99, current.longValue());
    }

    @Test
    @DisplayName("exchangeInContainer with VECTOR by position")
    void testExchangeInContainerVector() throws Exception {
        String key = "exch_vector_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(10), Variant.ofLong(20), Variant.ofLong(30));
        client.createVector(key, initialData).get();

        // exchangeInContainer: VECTOR[2] = 42 → returns OLD value 30
        Number oldVal = client.exchangeInContainer(key, 2, 42).get();
        assertEquals(30, oldVal.longValue());

        // Verify VECTOR[2] is now 42
        Variant current = client.getAt(key, 2).get();
        assertEquals(42, current.longValue());
    }

    // =========================================================================
    // 4. BITWISE OPERATIONS (AND, OR, XOR)
    // =========================================================================

    @Test
    @DisplayName("andInContainer with MAP by containerKey")
    void testAndInContainerMap() throws Exception {
        String key = "and_map_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(255)); // 0b11111111
        client.createMap(key, initialData).get();

        // andInContainer: val &= 0b00001111 → returns OLD value 255
        Long oldVal = client.andInContainer(key, p("val").getValue(), 0b00001111L).get();
        assertEquals(255L, oldVal.longValue());

        // Verify val is now 15 (0b00001111)
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(15L, current.longValue());
    }

    @Test
    @DisplayName("orInContainer with MAP by containerKey")
    void testOrInContainerMap() throws Exception {
        String key = "or_map_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(8)); // 0b1000
        client.createMap(key, initialData).get();

        // orInContainer: val |= 7 → returns OLD value 8
        Long oldVal = client.orInContainer(key, p("val").getValue(), 7L).get();
        assertEquals(8L, oldVal.longValue());

        // Verify val is now 15 (0b1111)
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(15L, current.longValue());
    }

    @Test
    @DisplayName("xorInContainer with MAP by containerKey")
    void testXorInContainerMap() throws Exception {
        String key = "xor_map_key_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(15)); // 0b1111
        client.createMap(key, initialData).get();

        // xorInContainer: val ^= 7 → returns OLD value 15
        Long oldVal = client.xorInContainer(key, p("val").getValue(), 7L).get();
        assertEquals(15L, oldVal.longValue());

        // Verify val is now 8 (0b1000)
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(8L, current.longValue());
    }

    @Test
    @DisplayName("andInContainer with VECTOR by position")
    void testAndInContainerVector() throws Exception {
        String key = "and_vector_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(255), Variant.ofLong(128), Variant.ofLong(64));
        client.createVector(key, initialData).get();

        // andInContainer: VECTOR[0] &= 0b00001111 → returns OLD value 255
        Long oldVal = client.andInContainer(key, 0, 0b00001111L).get();
        assertEquals(255L, oldVal.longValue());

        // Verify VECTOR[0] is now 15
        Variant current = client.getAt(key, 0).get();
        assertEquals(15L, current.longValue());
    }

    @Test
    @DisplayName("orInContainer with VECTOR by position")
    void testOrInContainerVector() throws Exception {
        String key = "or_vector_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(8), Variant.ofLong(16), Variant.ofLong(32));
        client.createVector(key, initialData).get();

        // orInContainer: VECTOR[0] |= 7 → returns OLD value 8
        Long oldVal = client.orInContainer(key, 0, 7L).get();
        assertEquals(8L, oldVal.longValue());

        // Verify VECTOR[0] is now 15
        Variant current = client.getAt(key, 0).get();
        assertEquals(15L, current.longValue());
    }

    @Test
    @DisplayName("xorInContainer with VECTOR by position")
    void testXorInContainerVector() throws Exception {
        String key = "xor_vector_pos_" + UUID.randomUUID();
        List<Variant> initialData = List.of(Variant.ofLong(15), Variant.ofLong(30), Variant.ofLong(45));
        client.createVector(key, initialData).get();

        // xorInContainer: VECTOR[0] ^= 7 → returns OLD value 15
        Long oldVal = client.xorInContainer(key, 0, 7L).get();
        assertEquals(15L, oldVal.longValue());

        // Verify VECTOR[0] is now 8
        Variant current = client.getAt(key, 0).get();
        assertEquals(8L, current.longValue());
    }

    // =========================================================================
    // 5. CHAINED OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Chained addInContainer operations")
    void testChainedAddInContainer() throws Exception {
        String key = "chained_add_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("counter"), Variant.ofLong(0));
        client.createMap(key, initialData).get();

        // addInContainer: counter += 10 → returns 0
        Number r1 = client.addInContainer(key, p("counter").getValue(), 10).get();
        assertEquals(0, r1.longValue());

        // addInContainer: counter += 20 → returns 10
        Number r2 = client.addInContainer(key, p("counter").getValue(), 20).get();
        assertEquals(10, r2.longValue());

        // addInContainer: counter += 5 → returns 30
        Number r3 = client.addInContainer(key, p("counter").getValue(), 5).get();
        assertEquals(30, r3.longValue());

        // Verify counter is now 35
        Variant current = client.getInContainer(key, p("counter").getValue()).get();
        assertEquals(35, current.longValue());
    }

    @Test
    @DisplayName("Chained addInContainer and subInContainer")
    void testChainedAddAndSubInContainer() throws Exception {
        String key = "chained_add_sub_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), Variant.ofLong(100));
        client.createMap(key, initialData).get();

        // addInContainer: val += 50 → returns 100
        Number r1 = client.addInContainer(key, p("val").getValue(), 50).get();
        assertEquals(100, r1.longValue());

        // subInContainer: val -= 30 → returns 150
        Number r2 = client.subInContainer(key, p("val").getValue(), 30).get();
        assertEquals(150, r2.longValue());

        // addInContainer: val += 10 → returns 120
        Number r3 = client.addInContainer(key, p("val").getValue(), 10).get();
        assertEquals(120, r3.longValue());

        // Verify val is now 130
        Variant current = client.getInContainer(key, p("val").getValue()).get();
        assertEquals(130, current.longValue());
    }

    // =========================================================================
    // 6. ERROR CASES — Non-numeric container fields
    // =========================================================================

    @Test
    @DisplayName("addInContainer on BYTES field - INVALID_ARGUMENT")
    void testAddInContainerOnStringField() throws Exception {
        String key = "err_add_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("hello")); // BYTES type
        client.createMap(key, initialData).get();

        // addInContainer on BYTES field → should throw INVALID_ARGUMENT
        try {
            client.addInContainer(key, p("name").getValue(), 5).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        // Verify name is still "hello" (unchanged)
        Variant current = client.getInContainer(key, p("name").getValue()).get();
        assertEquals("hello", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("subInContainer on BYTES field - INVALID_ARGUMENT")
    void testSubInContainerOnStringField() throws Exception {
        String key = "err_sub_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("test"));
        client.createMap(key, initialData).get();

        try {
            client.subInContainer(key, p("name").getValue(), 10).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getInContainer(key, p("name").getValue()).get();
        assertEquals("test", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("exchangeInContainer on BYTES field - INVALID_ARGUMENT")
    void testExchangeInContainerOnStringField() throws Exception {
        String key = "err_exch_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("world"));
        client.createMap(key, initialData).get();

        try {
            client.exchangeInContainer(key, p("name").getValue(), 42).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getInContainer(key, p("name").getValue()).get();
        assertEquals("world", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("andInContainer on BYTES field - INVALID_ARGUMENT")
    void testAndInContainerOnStringField() throws Exception {
        String key = "err_and_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("data"));
        client.createMap(key, initialData).get();

        try {
            client.andInContainer(key, p("name").getValue(), 15L).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getInContainer(key, p("name").getValue()).get();
        assertEquals("data", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("orInContainer on BYTES field - INVALID_ARGUMENT")
    void testOrInContainerOnStringField() throws Exception {
        String key = "err_or_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("info"));
        client.createMap(key, initialData).get();

        try {
            client.orInContainer(key, p("name").getValue(), 7L).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getInContainer(key, p("name").getValue()).get();
        assertEquals("info", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("xorInContainer on BYTES field - INVALID_ARGUMENT")
    void testXorInContainerOnStringField() throws Exception {
        String key = "err_xor_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("value"));
        client.createMap(key, initialData).get();

        try {
            client.xorInContainer(key, p("name").getValue(), 42L).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getInContainer(key, p("name").getValue()).get();
        assertEquals("value", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addInContainer on VECTOR with BYTES elements - INVALID_ARGUMENT")
    void testAddInContainerOnVectorWithStrings() throws Exception {
        String key = "err_add_vec_bytes_" + UUID.randomUUID();
        List<Variant> initialData = List.of(v("a"), v("b"), v("c"));
        client.createVector(key, initialData).get();

        try {
            client.addInContainer(key, 0, 10).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getAt(key, 0).get();
        assertEquals("a", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("andInContainer on VECTOR with BYTES elements - INVALID_ARGUMENT")
    void testAndInContainerOnVectorWithStrings() throws Exception {
        String key = "err_and_vec_bytes_" + UUID.randomUUID();
        List<Variant> initialData = List.of(v("x"), v("y"), v("z"));
        client.createVector(key, initialData).get();

        try {
            client.andInContainer(key, 1, 0xFFL).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }

        Variant current = client.getAt(key, 1).get();
        assertEquals("y", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }
}