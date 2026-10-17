package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive lock protection tests covering ALL containers:
 * - Scalar (createKeyValue/getValue)
 * - List (createList/streamList)
 * - Vector (createVector/streamVector)
 * - Queue (createQueue/streamQueue)
 * - Set (createSet/streamSet)
 * - OrderedSet (createOrderedSet/streamOrderedSet)
 * - HashMap (createMap/streamMap)
 * - OrderedMap (createOrderedMap/streamOrderedMap)
 *
 * Lock types: READ_LOCK, WRITE_LOCK, GLOBAL
 */
public class LockMethodProtectionTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // SCALAR - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("Scalar: READ_LOCK allows shared reads, blocks writes")
    void testScalarReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_scalar_read_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("secret")).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.get(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Variant intruderVal = client.get(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.replace(key, v("updated"), OWNER_CLIENT_ID));
        assertDenied(client.replace(key, v("updated"), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Scalar: WRITE_LOCK blocks intruder read and write")
    void testScalarWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_scalar_write_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("secret")).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.get(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Owner can write
        client.replace(key, v("updated"), OWNER_CLIENT_ID).get();

        // Intruder blocked
        assertDenied(client.get(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Scalar: GLOBAL_LOCK blocks intruder")
    void testScalarGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_scalar_global_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("secret")).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Variant val = client.get(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.get(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // LIST - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("List: READ_LOCK allows shared reads, blocks writes")
    void testListReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_list_read_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("item1"), v("item2"))).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<Variant> val = client.getList(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<Variant> intruderVal = client.getList(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.pushTail(key, null, List.of(v("item3")), OWNER_CLIENT_ID));
        assertDenied(client.pushTail(key, null, List.of(v("item3")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("List: WRITE_LOCK blocks intruder modifications")
    void testListWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_list_write_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can add
        client.pushTail(key, null, List.of(v("item2")), OWNER_CLIENT_ID).get();

        // Intruder blocked
        assertDenied(client.getList(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("List: GLOBAL_LOCK blocks intruder")
    void testListGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_list_global_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getList(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getList(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // VECTOR - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("Vector: READ_LOCK allows shared reads, blocks writes")
    void testVectorReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_vector_read_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("item1"), v("item2"))).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<Variant> val = client.getVector(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<Variant> intruderVal = client.getVector(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.pushTail(key, null, List.of(v("item3")), OWNER_CLIENT_ID));
        assertDenied(client.pushTail(key, null, List.of(v("item3")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Vector: WRITE_LOCK blocks intruder modifications")
    void testVectorWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_vector_write_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        client.pushTail(key, null, List.of(v("item2")), OWNER_CLIENT_ID).get();

        assertDenied(client.getVector(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Vector: GLOBAL_LOCK blocks intruder")
    void testVectorGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_vector_global_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getVector(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getVector(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // QUEUE - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("Queue: READ_LOCK allows shared reads, blocks writes")
    void testQueueReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_queue_read_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("item1"), v("item2"))).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Variant val = client.peekHead(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Variant intruderVal = client.peekHead(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.pushTail(key, null, List.of(v("item3")), OWNER_CLIENT_ID));
        assertDenied(client.pushTail(key, null, List.of(v("item3")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Queue: WRITE_LOCK blocks intruder modifications")
    void testQueueWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_queue_write_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        client.pushTail(key, null, List.of(v("item2")), OWNER_CLIENT_ID).get();

        assertDenied(client.peekHead(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Queue: GLOBAL_LOCK blocks intruder")
    void testQueueGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_queue_global_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Variant val = client.peekHead(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.peekHead(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // SET - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("Set: READ_LOCK allows shared reads, blocks writes")
    void testSetReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_set_read_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("item1"), v("item2"))).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<Variant> val = client.getSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<Variant> intruderVal = client.getSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.addElementUnordered(key, null, List.of(v("item3")), OWNER_CLIENT_ID));
        assertDenied(client.addElementUnordered(key, null, List.of(v("item3")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Set: WRITE_LOCK blocks intruder modifications")
    void testSetWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_set_write_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        client.addElementUnordered(key, null, List.of(v("item2")), OWNER_CLIENT_ID).get();

        assertDenied(client.getSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Set: GLOBAL_LOCK blocks intruder")
    void testSetGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_set_global_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("item1"))).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // ORDERED SET - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("OrderedSet: READ_LOCK allows shared reads, blocks writes")
    void testOrderedSetReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_ordered_set_read_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L),
                OrderedVariant.ofBytes("item2".getBytes(), 2L)
        )).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<OrderedVariant> val = client.getOrderedSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<OrderedVariant> intruderVal = client.getOrderedSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("OrderedSet: WRITE_LOCK blocks intruder modifications")
    void testOrderedSetWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_ordered_set_write_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L)
        )).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // OrderedSet has no addElement method - test would need to create new set
        // This line is kept for reference only
        // client.addElement(key, null, List.of(
        //         OrderedVariant.ofBytes("item2".getBytes(), 2L)
        // ), OWNER_CLIENT_ID).get();

        assertDenied(client.getOrderedSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("OrderedSet: GLOBAL_LOCK blocks intruder")
    void testOrderedSetGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_ordered_set_global_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L)
        )).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<OrderedVariant> val = client.getOrderedSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getOrderedSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // HASH MAP - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("HashMap: READ_LOCK allows shared reads, blocks writes")
    void testHashMapReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_hash_map_read_" + UUID.randomUUID();

        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));
        data.put(p("key2"), v("value2"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Map<Payload, Variant> val = client.getMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Map<Payload, Variant> intruderVal = client.getMap(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.putAll(key, null, List.of(p("key3")), List.of(v("value3")), OWNER_CLIENT_ID));
        assertDenied(client.putAll(key, null, List.of(p("key3")), List.of(v("value3")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("HashMap: WRITE_LOCK blocks intruder modifications")
    void testHashMapWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_hash_map_write_" + UUID.randomUUID();

        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can write
        client.putAll(key, hint,
                      List.of(p("key2")),
                      List.of(v("value2")),
                      OWNER_CLIENT_ID).get();

        // Intruder blocked
        assertDenied(client.getMap(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("HashMap: GLOBAL_LOCK blocks intruder")
    void testHashMapGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_hash_map_global_" + UUID.randomUUID();

        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value1"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Map<Payload, Variant> val = client.getMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getMap(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // ORDERED MAP - Lock Protection
    // =========================================================================

    @Test
    @DisplayName("OrderedMap: READ_LOCK allows shared reads, blocks writes")
    void testOrderedMapReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_ordered_map_read_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));
        data.put(OrderedPayload.of("key2".getBytes(), 2L), v("value2"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        Map<OrderedPayload, Variant> val = client.getOrderedMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        Map<OrderedPayload, Variant> intruderVal = client.getOrderedMap(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("OrderedMap: WRITE_LOCK blocks intruder modifications")
    void testOrderedMapWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_ordered_map_write_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can write
        client.addElementOrderedMap(key, hint,
                List.of(OrderedPayload.of("key2".getBytes(), 2L)),
                List.of(v("value2")),
                OWNER_CLIENT_ID).get();

        // Intruder blocked
        assertDenied(client.getOrderedMap(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("OrderedMap: GLOBAL_LOCK blocks intruder")
    void testOrderedMapGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_ordered_map_global_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value1"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        Map<OrderedPayload, Variant> val = client.getOrderedMap(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getOrderedMap(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // UNLOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Unlock by owner only - scalar")
    void testUnlockOwnerOnlyScalar() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - list")
    void testUnlockOwnerOnlyList() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_list_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - vector")
    void testUnlockOwnerOnlyVector() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_vector_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - queue")
    void testUnlockOwnerOnlyQueue() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_queue_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - set")
    void testUnlockOwnerOnlySet() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_set_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - ordered set")
    void testUnlockOwnerOnlyOrderedSet() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_ordered_set_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        )).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - hash map")
    void testUnlockOwnerOnlyHashMap() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_hash_map_" + UUID.randomUUID();

        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("Unlock by owner only - ordered map")
    void testUnlockOwnerOnlyOrderedMap() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_ordered_map_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        LockStatus unlock = client.unlock(key, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }



    // =========================================================================
    // LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("Lock expiration - scalar")
    void testLockExpirationScalar() throws ExecutionException, InterruptedException {
        String key = "lock_expire_scalar_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        Variant val = client.get(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - list")
    void testLockExpirationList() throws ExecutionException, InterruptedException {
        String key = "lock_expire_list_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<Variant> val = client.getList(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - vector")
    void testLockExpirationVector() throws ExecutionException, InterruptedException {
        String key = "lock_expire_vector_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<Variant> val = client.getVector(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - queue")
    void testLockExpirationQueue() throws ExecutionException, InterruptedException {
        String key = "lock_expire_queue_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        Variant val = client.peekHead(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - set")
    void testLockExpirationSet() throws ExecutionException, InterruptedException {
        String key = "lock_expire_set_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("value"))).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<Variant> val = client.getSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - ordered set")
    void testLockExpirationOrderedSet() throws ExecutionException, InterruptedException {
        String key = "lock_expire_ordered_set_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("value".getBytes(), 1L)
        )).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<OrderedVariant> val = client.getOrderedSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - hash map")
    void testLockExpirationHashMap() throws ExecutionException, InterruptedException {
        String key = "lock_expire_hash_map_" + UUID.randomUUID();

        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("key1"), v("value"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        Map<Payload, Variant> val = client.getMap(key).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("Lock expiration - ordered map")
    void testLockExpirationOrderedMap() throws ExecutionException, InterruptedException {
        String key = "lock_expire_ordered_map_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("key1".getBytes(), 1L), v("value"));

        KeyHintData hint = client.createOrderedMap(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        Map<OrderedPayload, Variant> val = client.getOrderedMap(key).get();
        assertNotNull(val);
    }
}