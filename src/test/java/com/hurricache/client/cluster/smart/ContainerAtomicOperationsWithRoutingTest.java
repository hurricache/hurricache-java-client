/*
 * HurriCache Java Client - Container Atomic Operations Routing Tests
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
package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
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
 * Tests for container atomic operations routing (MASTER/BACKUP) with FastCacheAsyncSmartClient.
 * Verifies that operations on MASTER are properly propagated to BACKUP.
 */
public class ContainerAtomicOperationsWithRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. ADD IN CONTAINER WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addInContainer on MASTER, validate on BACKUP")
    void testAddInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "add_route_map_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("f1"), vi(10));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        // Operation on MASTER
        client.setMode(Mode.MASTER);
        Number oldVal = client.addInContainer(key, p("f1").getValue(), 5).get();
        assertEquals(10, oldVal.longValue());
        Thread.sleep(500);

        // Validate on BACKUP
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("f1").getValue()).get();
        assertEquals(15, current.longValue());
    }

    @Test
    @DisplayName("addInContainer with VECTOR by pos on MASTER, validate on BACKUP")
    void testAddInContainerVectorOnMasterValidateOnBackup() throws Exception {
        String key = "add_route_vector_" + UUID.randomUUID();
        List<Variant> initialData = List.of(vi(100), vi(200), vi(300));
        client.createVector(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number oldVal = client.addInContainer(key, 1, 50).get();
        assertEquals(200, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getAt(key, 1).get();
        assertEquals(250, current.longValue());
    }

    // =========================================================================
    // 2. SUB IN CONTAINER WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("subInContainer on MASTER, validate on BACKUP")
    void testSubInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "sub_route_map_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), vi(100));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number oldVal = client.subInContainer(key, p("val").getValue(), 25).get();
        assertEquals(100, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("val").getValue()).get();
        assertEquals(75, current.longValue());
    }

    @Test
    @DisplayName("subInContainer with VECTOR by pos on MASTER, validate on BACKUP")
    void testSubInContainerVectorOnMasterValidateOnBackup() throws Exception {
        String key = "sub_route_vector_" + UUID.randomUUID();
        List<Variant> initialData = List.of(vi(50), vi(100), vi(150));
        client.createVector(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number oldVal = client.subInContainer(key, 1, 30).get();
        assertEquals(100, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getAt(key, 1).get();
        assertEquals(70, current.longValue());
    }

    // =========================================================================
    // 3. EXCHANGE IN CONTAINER WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("exchangeInContainer on MASTER, validate on BACKUP")
    void testExchangeInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "exch_route_map_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), vi(50));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number oldVal = client.exchangeInContainer(key, p("val").getValue(), 99).get();
        assertEquals(50, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("val").getValue()).get();
        assertEquals(99, current.longValue());
    }

    @Test
    @DisplayName("exchangeInContainer with VECTOR by pos on MASTER, validate on BACKUP")
    void testExchangeInContainerVectorOnMasterValidateOnBackup() throws Exception {
        String key = "exch_route_vector_" + UUID.randomUUID();
        List<Variant> initialData = List.of(vi(10), vi(20), vi(30));
        client.createVector(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number oldVal = client.exchangeInContainer(key, 2, 42).get();
        assertEquals(30, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getAt(key, 2).get();
        assertEquals(42, current.longValue());
    }

    // =========================================================================
    // 4. BITWISE OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("andInContainer on MASTER, validate on BACKUP")
    void testAndInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "and_route_map_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), vi(255));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long oldVal = client.andInContainer(key, p("val").getValue(), 0b00001111L).get();
        assertEquals(255L, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("val").getValue()).get();
        assertEquals(15L, current.longValue());
    }

    @Test
    @DisplayName("orInContainer on MASTER, validate on BACKUP")
    void testOrInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "or_route_map_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), vi(8));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long oldVal = client.orInContainer(key, p("val").getValue(), 7L).get();
        assertEquals(8L, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("val").getValue()).get();
        assertEquals(15L, current.longValue());
    }

    @Test
    @DisplayName("xorInContainer on MASTER, validate on BACKUP")
    void testXorInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "xor_route_map_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), vi(15));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long oldVal = client.xorInContainer(key, p("val").getValue(), 7L).get();
        assertEquals(15L, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("val").getValue()).get();
        assertEquals(8L, current.longValue());
    }

    @Test
    @DisplayName("andInContainer with VECTOR by pos on MASTER, validate on BACKUP")
    void testAndInContainerVectorOnMasterValidateOnBackup() throws Exception {
        String key = "and_route_vector_" + UUID.randomUUID();
        List<Variant> initialData = List.of(vi(255), vi(128), vi(64));
        client.createVector(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long oldVal = client.andInContainer(key, 0, 0b00001111L).get();
        assertEquals(255L, oldVal.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getAt(key, 0).get();
        assertEquals(15L, current.longValue());
    }

    // =========================================================================
    // 5. ERROR CASES WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addInContainer on BYTES field - INVALID_ARGUMENT on MASTER")
    void testAddInContainerOnStringFieldMaster() throws Exception {
        String key = "err_route_add_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("name"), v("test"));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        try {
            client.addInContainer(key, p("name").getValue(), 10).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }
        Thread.sleep(500);

        // Verify name is still "test" on BACKUP
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("name").getValue()).get();
        assertEquals("test", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("andInContainer on BYTES field - INVALID_ARGUMENT on MASTER")
    void testAndInContainerOnStringFieldMaster() throws Exception {
        String key = "err_route_and_bytes_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("data"), v("bytes"));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        try {
            client.andInContainer(key, p("data").getValue(), 15L).get();
            fail("Expected INVALID_ARGUMENT exception");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        }
        Thread.sleep(500);

        // Verify data is still "bytes" on BACKUP
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("data").getValue()).get();
        assertEquals("bytes", new String(current.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. ATOMIC OPERATIONS WITH LOCKS
    // =========================================================================

    @Test
    @DisplayName("addInContainer with WRITE_LOCK on MASTER, validate on BACKUP")
    void testAddInContainerWithLock() throws Exception {
        String key = "lock_add_route_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("f1"), vi(10));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, null, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // addInContainer on MASTER
        Number oldVal = client.addInContainer(key, p("f1").getValue(), 5, 100).get();
        assertEquals(10, oldVal.longValue());
        Thread.sleep(500);

        // Validate on BACKUP
        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, null, p("f1").getValue(), 100).get();
        assertEquals(15, current.longValue());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, null, 100).get();
    }

    // =========================================================================
    // 7. CHAINED OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Chained addInContainer and subInContainer on MASTER, validate on BACKUP")
    void testChainedAddAndSubInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "chained_route_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("val"), vi(100));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        
        Number r1 = client.addInContainer(key, p("val").getValue(), 50).get();
        assertEquals(100, r1.longValue());
        Thread.sleep(500);

        Number r2 = client.subInContainer(key, p("val").getValue(), 30).get();
        assertEquals(150, r2.longValue());
        Thread.sleep(500);

        Number r3 = client.addInContainer(key, p("val").getValue(), 10).get();
        assertEquals(120, r3.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("val").getValue()).get();
        assertEquals(130, current.longValue());
    }

    @Test
    @DisplayName("Multiple addInContainer operations on MASTER, validate on BACKUP")
    void testMultipleAddInContainerOnMasterValidateOnBackup() throws Exception {
        String key = "multi_add_route_" + UUID.randomUUID();
        Map<Payload, Variant> initialData = new LinkedHashMap<>();
        initialData.put(p("counter"), vi(0));
        client.createMap(key, initialData).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        
        Number r1 = client.addInContainer(key, p("counter").getValue(), 10).get();
        assertEquals(0, r1.longValue());
        Thread.sleep(500);

        Number r2 = client.addInContainer(key, p("counter").getValue(), 20).get();
        assertEquals(10, r2.longValue());
        Thread.sleep(500);

        Number r3 = client.addInContainer(key, p("counter").getValue(), 5).get();
        assertEquals(30, r3.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).getInContainer(key, p("counter").getValue()).get();
        assertEquals(35, current.longValue());
    }
}
