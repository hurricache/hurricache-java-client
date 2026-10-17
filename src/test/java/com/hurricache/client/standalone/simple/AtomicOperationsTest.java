package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AtomicOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. ATOMIC EXCHANGE
    // =========================================================================

    @Test
    @DisplayName("atomicExchange with int value")
    void testAtomicExchangeInt() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_int_" + UUID.randomUUID();

        // Create with int
        KeyHintData hint = client.create(key, vi(100)).get();

        // Exchange to new value
        Number previous = client.exchange(key, 200).get();
        assertNotNull(previous);
        assertEquals(100, previous.longValue());

        // Verify new value
        Variant current = client.get(key).get();
        assertEquals(200, current.longValue());
        assert current.isInt() : "Expected INT type";
    }

    @Test
    @DisplayName("atomicExchange with double value - int is INVALID_ARGUMENT")
    void testAtomicExchangeDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_double_" + UUID.randomUUID();

        // Create with double
        KeyHintData hint = client.create(key, vd(3.14)).get();

        // Exchange double with int - should return INVALID_ARGUMENT
        assertInvalidArgument(client.exchange(key, 1L));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
        assertEquals(3.14, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicExchange with double value - double to double works")
    void testAtomicExchangeDoubleWithDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_double_double_" + UUID.randomUUID();

        // Create with double
        KeyHintData hint = client.create(key, vd(3.14)).get();

        // Exchange double with double - should work
        Number previous = client.exchange(key, 2.71).get();
        assertNotNull(previous);

        // Verify new value
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
        assertEquals(2.71, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicExchange with bytes type - bytes to int is INVALID_ARGUMENT")
    void testAtomicExchangeBytesToInt() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_bytes_" + UUID.randomUUID();

        // Create with bytes
        KeyHintData hint = client.create(key, v("hello")).get();

        // Exchange bytes with int - should return INVALID_ARGUMENT
        assertInvalidArgument(client.exchange(key, 42));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 2. ATOMIC ADD
    // =========================================================================

    @Test
    @DisplayName("atomicAdd with int value - returns OLD value")
    void testAtomicAddInt() throws ExecutionException, InterruptedException {
        String key = "atomic_add_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();

        // atomicAdd returns OLD value (like std::atomic::fetch_add)
        Number result = client.add(key, 5).get();
        assertNotNull(result);
        assertEquals(10, result.longValue()); // Returns OLD value

        // Verify new value is correct
        Variant current = client.get(key).get();
        assertEquals(15, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicAdd with negative delta - returns OLD value")
    void testAtomicAddNegative() throws ExecutionException, InterruptedException {
        String key = "atomic_add_negative_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();

        // Returns OLD value
        Number result = client.add(key, -3).get();
        assertNotNull(result);
        assertEquals(10, result.longValue()); // Returns OLD value

        // Verify new value
        Variant current = client.get(key).get();
        assertEquals(7, current.longValue());
    }

    @Test
    @DisplayName("atomicAdd with double value - adding int is INVALID_ARGUMENT")
    void testAtomicAddDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_add_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(10.5)).get();

        // atomicAdd double with int - should return INVALID_ARGUMENT
        assertInvalidArgument(client.add(key, 1));

        // Verify value remains unchanged
        Variant current = client.get(key).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(10.5, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicAdd with double value - adding double works")
    void testAtomicAddDoubleWithDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_add_double_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(10.5)).get();

        // atomicAdd double with double - should work
        Number result = client.add(key, 2.5).get();
        assertNotNull(result);

        // Verify new value
        Variant current = client.get(key).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(13.0, current.doubleValue(), 0.001);
    }

    // =========================================================================
    // 3. ATOMIC SUB
    // =========================================================================

    @Test
    @DisplayName("atomicSub with int value - returns OLD value")
    void testAtomicSubInt() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();

        // atomicSub returns OLD value
        Number result = client.atomicSub(key, 25).get();
        assertNotNull(result);
        assertEquals(100, result.longValue()); // Returns OLD value

        // Verify new value is correct
        Variant current = client.get(key).get();
        assertEquals(75, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicSub with double value - subtracting int is INVALID_ARGUMENT")
    void testAtomicSubDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(50.0)).get();

        // atomicSub double with int - should return INVALID_ARGUMENT
        assertInvalidArgument(client.atomicSub(key, 1));

        // Verify value remains unchanged
        Variant current = client.get(key).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(50.0, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicSub with double value - subtracting double works")
    void testAtomicSubDoubleWithDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_double_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(50.0)).get();

        // atomicSub double with double - should work
        Number result = client.atomicSub(key, 5.5).get();
        assertNotNull(result);

        // Verify new value through getValue
        Variant current = client.get(key).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(44.5, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicSub goes negative - returns OLD value")
    void testAtomicSubNegative() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_negative_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();

        // Returns OLD value
        Number result = client.atomicSub(key, 30).get();
        assertNotNull(result);
        assertEquals(10, result.longValue()); // Returns OLD value

        // Verify new value
        Variant current = client.get(key).get();
        assertEquals(-20, current.longValue());
    }

    // =========================================================================
    // 4. ATOMIC BITWISE AND
    // =========================================================================

    @Test
    @DisplayName("atomicAnd with int value - returns OLD value")
    void testAtomicAnd() throws ExecutionException, InterruptedException {
        String key = "atomic_and_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(255)).get();

        // atomicAnd returns OLD value (like fetch_and)
        Long result = client.and(key, 15).get();
        assertNotNull(result);
        assertEquals(255, result.longValue()); // Returns OLD value

        // Verify new value through getValue
        Variant current = client.get(key).get();
        assertEquals(15, current.longValue()); // 255 & 15 = 15
    }

    @Test
    @DisplayName("atomicAnd with mask - returns OLD value")
    void testAtomicAndMask() throws ExecutionException, InterruptedException {
        String key = "atomic_and_mask_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(255)).get();

        // atomicAnd returns OLD value
        Long result = client.and(key, 0b00001111).get();
        assertEquals(255, result.longValue()); // Returns OLD value

        // Verify new value through getValue
        Variant current = client.get(key).get();
        assertEquals(15, current.longValue()); // 255 & 0b00001111 = 15
    }

    // =========================================================================
    // 5. ATOMIC BITWISE OR
    // =========================================================================

    @Test
    @DisplayName("atomicOr with int value - returns OLD value")
    void testAtomicOr() throws ExecutionException, InterruptedException {
        String key = "atomic_or_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(8)).get();

        // atomicOr returns OLD value
        Long result = client.or(key, 7).get();
        assertNotNull(result);
        assertEquals(8, result.longValue()); // Returns OLD value

        // Verify new value through getValue
        Variant current = client.get(key).get();
        assertEquals(15, current.longValue()); // 8 | 7 = 15
    }



    // =========================================================================
    // 6. ATOMIC BITWISE XOR
    // =========================================================================

    @Test
    @DisplayName("atomicXor with int value - returns OLD value")
    void testAtomicXor() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(15)).get();

        // atomicXor returns OLD value
        Long result = client.xor(key, 7).get();
        assertNotNull(result);
        assertEquals(15, result.longValue()); // Returns OLD value

        // Verify new value through getValue
        Variant current = client.get(key).get();
        assertEquals(8, current.longValue()); // 15 ^ 7 = 8
    }

    @Test
    @DisplayName("atomicXor with double - INVALID_ARGUMENT (bitwise on double)")
    void testAtomicXorDoubleInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(2.71)).get();

        // Bitwise XOR on double - should return INVALID_ARGUMENT
        assertInvalidArgument(client.xor(key, 42));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
    }

    // =========================================================================
    // 7. ATOMIC COMPARE AND SET (CAS)
    // =========================================================================

    @Test
    @DisplayName("atomicCompareAndSet - success case")
    void testAtomicCompareAndSetSuccess() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_success_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();

        AtomicCasRes result = client.compareAndSet(key, 100, 200).get();
        assertNotNull(result);
        assertTrue(result.getResult());

        Variant current = client.get(key).get();
        assertEquals(200, current.longValue());
        assert current.isInt() : "Expected INT type";
    }

    @Test
    @DisplayName("atomicCompareAndSet - failure case")
    void testAtomicCompareAndSetFailure() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_failure_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();

        AtomicCasRes result = client.compareAndSet(key, 999, 200).get();
        assertNotNull(result);
        assertTrue(!result.getResult());

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertEquals(100, current.longValue());
        assert current.isInt() : "Expected INT type";
    }

    @Test
    @DisplayName("atomicCompareAndSet with double value - double to double works")
    void testAtomicCompareAndSetDouble() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();

        // CAS with double values - should succeed
        AtomicCasRes result = client.compareAndSet(key, 3.14, 2.71).get();
        assertNotNull(result);
        assertTrue(result.getResult());

        // Verify new value
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
        assertEquals(2.71, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicCompareAndSet with double - int expected returns INVALID_ARGUMENT")
    void testAtomicCompareAndSetDoubleWithIntExpected() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_double_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();

        // CAS with int expected on double value - should return INVALID_ARGUMENT (type mismatch)
        assertInvalidArgument(client.compareAndSet(key, 0L, 1L));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
        assertEquals(3.14, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicCompareAndSet with bytes type - bytes to long is INVALID_ARGUMENT")
    void testAtomicCompareAndSetBytes() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_bytes_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();

        // CAS with bytes - expecting long is INVALID_ARGUMENT
        assertInvalidArgument(client.compareAndSet(key, 0L, 1L));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 8. ATOMIC OPERATIONS WITH LOCKS
    // =========================================================================

    @Test
    @DisplayName("atomicAdd with WRITE_LOCK - returns OLD value")
    void testAtomicAddWithWriteLock() throws ExecutionException, InterruptedException {
        String key = "atomic_add_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // atomicAdd returns OLD value (like fetch_add)
        Number result = client.add(key, hint, 5, OWNER_CLIENT_ID).get();
        assertNotNull(result);
        assertEquals(10, result.longValue()); // Returns OLD value

        // Verify new value is correct
        Variant current = client.get(key, OWNER_CLIENT_ID).get();
        assertEquals(15, current.longValue());

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("atomicExchange with READ_LOCK - denied")
    void testAtomicExchangeWithReadLock() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Intruder cannot perform atomic operations
        assertDenied(client.exchange(key, hint, 20, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("atomicCompareAndSet with WRITE_LOCK")
    void testAtomicCompareAndSetWithWriteLock() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        AtomicCasRes result = client.compareAndSet(key, hint, 100, 200, OWNER_CLIENT_ID).get();
        assertNotNull(result);
        assertTrue(result.getResult());

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    // =========================================================================
    // 9. ATOMIC OPERATIONS TYPE SWITCHING
    // =========================================================================

    @Test
    @DisplayName("atomicAdd after type switch bytes→int - returns OLD value")
    void testAtomicAddAfterTypeSwitch() throws ExecutionException, InterruptedException {
        String key = "atomic_type_switch_" + UUID.randomUUID();

        // Create with bytes
        KeyHintData hint = client.create(key, v("hello")).get();

        // Switch to int explicitly
        Variant variant = client.replace(key, vi(50)).get();

        // atomicAdd int with int (compatible) - returns OLD value
        Number result = client.add(key, 10).get();
        assertNotNull(result);
        assertEquals(50, result.longValue()); // Returns OLD value

        // Verify new value
        Variant current = client.get(key).get();
        assertEquals(60, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicSub after type switch int→double - returns OLD value")
    void testAtomicSubAfterTypeSwitch() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_type_switch_" + UUID.randomUUID();

        // Create with int
        KeyHintData hint = client.create(key, vi(100)).get();

        // Switch to double explicitly
        client.replace(key, vd(50.5)).get();

        // atomicSub double with double - returns OLD value
        Number result = client.atomicSub(key, 5.5).get();
        assertNotNull(result);

        // Verify new value through getValue
        Variant current = client.get(key).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(45.0, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicAdd on bytes - INVALID_ARGUMENT (no arithmetic on bytes)")
    void testAtomicAddOnBytesInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_add_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();

        // atomicAdd on bytes - should return INVALID_ARGUMENT
        assertInvalidArgument(client.add(key, 10));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }

    @Test
    @DisplayName("atomicAnd on double - INVALID_ARGUMENT (bitwise on double)")
    void testAtomicAndOnDoubleInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_and_double_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();

        // atomicAnd on double - should return INVALID_ARGUMENT
        assertInvalidArgument(client.and(key, 15));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
    }

    @Test
    @DisplayName("atomicAnd on bytes - INVALID_ARGUMENT (bitwise on bytes)")
    void testAtomicAndOnBytesInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_and_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();

        // atomicAnd on bytes - should return INVALID_ARGUMENT
        assertInvalidArgument(client.and(key, 15));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }

    @Test
    @DisplayName("atomicOr on double - INVALID_ARGUMENT (bitwise on double)")
    void testAtomicOrDoubleInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_or_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();

        // Bitwise OR on double - should return INVALID_ARGUMENT
        assertInvalidArgument(client.or(key, 1));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
    }

    @Test
    @DisplayName("atomicOr on bytes - INVALID_ARGUMENT (bitwise on bytes)")
    void testAtomicOrBytesInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_or_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();

        // Bitwise OR on bytes - should return INVALID_ARGUMENT
        assertInvalidArgument(client.or(key, 1));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }


    @Test
    @DisplayName("atomicXor on bytes - INVALID_ARGUMENT (bitwise on bytes)")
    void testAtomicXorBytesInvalid() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();

        // Bitwise XOR on bytes - should return INVALID_ARGUMENT
        assertInvalidArgument(client.xor(key, 42));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 10. CHAINED ATOMIC OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Multiple atomicAdd operations - each returns OLD value")
    void testMultipleAtomicAdd() throws ExecutionException, InterruptedException {
        String key = "atomic_chained_add_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(0)).get();

        // atomicAdd returns OLD value
        Number r1 = client.add(key, 10).get();
        assertEquals(0, r1.longValue()); // Returns OLD value (0)

        Number r2 = client.add(key, 20).get();
        assertEquals(10, r2.longValue()); // Returns OLD value (0+10=10)

        Number r3 = client.add(key, 5).get();
        assertEquals(30, r3.longValue()); // Returns OLD value (10+20=30)

        // Verify final value
        Variant current = client.get(key).get();
        assertEquals(35, current.longValue()); // 0+10+20+5=35
    }

    @Test
    @DisplayName("Chained atomicAdd and atomicSub - same type")
    void testChainedAddAndSub() throws ExecutionException, InterruptedException {
        String key = "atomic_chained_add_sub_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();

        // All operations on int type (compatible)
        Number r1 = client.add(key, 50).get();
        assertEquals(100, r1.longValue()); // OLD value

        Number r2 = client.atomicSub(key, 30).get();
        assertEquals(150, r2.longValue()); // OLD value

        Number r3 = client.add(key, 10).get();
        assertEquals(120, r3.longValue()); // OLD value

        // Verify final value
        Variant current = client.get(key).get();
        assertEquals(130, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicExchange followed by atomicCompareAndSet")
    void testExchangeThenCas() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_cas_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();

        // Exchange
        Number prev = client.exchange(key, 20).get();
        assertEquals(10, prev.longValue());

        // CAS should succeed with new value
        AtomicCasRes cas = client.compareAndSet(key, 100, 20).get();
        assertTrue(!cas.getResult()); // 100 != 20, so should fail

        // Try correct expected value
        cas = client.compareAndSet(key, 20, 30).get();
        assertTrue(cas.getResult());

        Variant current = client.get(key).get();
        assertEquals(30, current.longValue());
        assert current.isInt() : "Expected INT type";
    }
}