package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Atomic operations.
 * Mirrors AtomicOperationsTest for cluster routing.
 */
public class SmartClientAtomicRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. ATOMIC EXCHANGE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicExchange with int value on MASTER, validate on BACKUP")
    void testAtomicExchangeIntOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_int_" + UUID.randomUUID();

        // Create with int
        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // Exchange on MASTER
        client.setMode(Mode.MASTER);
        Number previous = client.exchange(key, hint, 200).get();
        assertNotNull(previous);
        assertEquals(100, previous.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(200, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicExchange with double value on MASTER, validate on BACKUP")
    void testAtomicExchangeDoubleOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_double_" + UUID.randomUUID();

        // Create with double
        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        // Exchange double with double on MASTER
        client.setMode(Mode.MASTER);
        Number previous = client.exchange(key, hint, 2.71).get();
        assertNotNull(previous);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
        assertEquals(2.71, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicExchange with double value - int is INVALID_ARGUMENT on MASTER")
    void testAtomicExchangeDoubleWithIntInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_double_" + UUID.randomUUID();

        // Create with double on MASTER
        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        // Exchange double with int on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.exchange(key, hint, 1L).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
        assertEquals(3.14, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicExchange with bytes type - bytes to int is INVALID_ARGUMENT on MASTER")
    void testAtomicExchangeBytesToIntInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_bytes_" + UUID.randomUUID();

        // Create with bytes on MASTER
        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // Exchange bytes with int on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.exchange(key, hint, 42).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 2. ATOMIC ADD WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicAdd with int value - returns OLD value on MASTER, validate on BACKUP")
    void testAtomicAddIntReturnsOldOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_add_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // atomicAdd returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.add(key, hint, 5).get();
        assertNotNull(result);
        assertEquals(10, result.longValue()); // Returns OLD value
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(15, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicAdd with double value - adding int is INVALID_ARGUMENT on MASTER")
    void testAtomicAddDoubleWithIntInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_add_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(10.5)).get();
        Thread.sleep(500);

        // atomicAdd double with int on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.add(key, hint, 1).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Verify value remains unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(10.5, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicAdd on bytes - INVALID_ARGUMENT on MASTER")
    void testAtomicAddOnBytesInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_add_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // atomicAdd on bytes on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.add(key, hint, 10).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 3. ATOMIC SUB WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicAdd with int value on MASTER, validate on BACKUP")
    void testAtomicAddIntOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_add_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // atomicAdd returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.add(key, hint, 5).get();
        assertNotNull(result);
        assertEquals(10, result.longValue()); // Returns OLD value
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(15, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicAdd with negative delta on MASTER, validate on BACKUP")
    void testAtomicAddNegativeOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_add_negative_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // Returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.add(key, hint, -3).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(7, current.longValue());
    }

    @Test
    @DisplayName("atomicAdd with double value on MASTER, validate on BACKUP")
    void testAtomicAddDoubleOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_add_double_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(10.5)).get();
        Thread.sleep(500);

        // atomicAdd double with double on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.add(key, hint, 2.5).get();
        assertNotNull(result);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(13.0, current.doubleValue(), 0.001);
    }

    // =========================================================================
    // 3. ATOMIC SUB WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicSub with int value on MASTER, validate on BACKUP")
    void testAtomicSubIntOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // atomicSub returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.sub(key, hint, 25).get();
        assertNotNull(result);
        assertEquals(100, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(75, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicSub with double value on MASTER, validate on BACKUP")
    void testAtomicSubDoubleOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_double_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(50.0)).get();
        Thread.sleep(500);

        // atomicSub double with double on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.sub(key, hint, 5.5).get();
        assertNotNull(result);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(44.5, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicSub goes negative on MASTER, validate on BACKUP")
    void testAtomicSubNegativeOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_negative_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // Returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Number result = client.sub(key, hint, 30).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(-20, current.longValue());
    }

    // =========================================================================
    // 4. ATOMIC BITWISE AND WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicAnd with int value on MASTER, validate on BACKUP")
    void testAtomicAndOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_and_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(255)).get();
        Thread.sleep(500);

        // atomicAnd returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Long result = client.and(key, hint, 15).get();
        assertNotNull(result);
        assertEquals(255, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(15, current.longValue()); // 255 & 15 = 15
    }

    @Test
    @DisplayName("atomicAnd with mask on MASTER, validate on BACKUP")
    void testAtomicAndMaskOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_and_mask_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(255)).get();
        Thread.sleep(500);

        // atomicAnd returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Long result = client.and(key, hint, 0b00001111).get();
        assertEquals(255, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(15, current.longValue()); // 255 & 0b00001111 = 15
    }

    @Test
    @DisplayName("atomicAnd on double - INVALID_ARGUMENT on MASTER")
    void testAtomicAndOnDoubleInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_and_double_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        // atomicAnd on double on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.and(key, hint, 15).get();
            fail("Expected exception for bitwise on double");
        } catch (ExecutionException e) {
            // Expected
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
    }

    @Test
    @DisplayName("atomicAnd on bytes - INVALID_ARGUMENT on MASTER")
    void testAtomicAndOnBytesInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_and_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // atomicAnd on bytes on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.and(key, hint, 15).get();
            fail("Expected exception for bitwise on bytes");
        } catch (ExecutionException e) {
            // Expected
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 5. ATOMIC BITWISE OR WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicOr with int value on MASTER, validate on BACKUP")
    void testAtomicOrOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_or_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(8)).get();
        Thread.sleep(500);

        // atomicOr returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Long result = client.or(key, hint, 7).get();
        assertNotNull(result);
        assertEquals(8, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(15, current.longValue()); // 8 | 7 = 15
    }

    @Test
    @DisplayName("atomicOr on double - INVALID_ARGUMENT on MASTER")
    void testAtomicOrOnDoubleInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_or_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        // Bitwise OR on double on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.or(key, hint, 1).get();
            fail("Expected exception for bitwise on double");
        } catch (ExecutionException e) {
            // Expected
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
    }

    @Test
    @DisplayName("atomicOr on bytes - INVALID_ARGUMENT on MASTER")
    void testAtomicOrOnBytesInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_or_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // Bitwise OR on bytes on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.or(key, hint, 1).get();
            fail("Expected exception for bitwise on bytes");
        } catch (ExecutionException e) {
            // Expected
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 6. ATOMIC BITWISE XOR WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicXor with int value on MASTER, validate on BACKUP")
    void testAtomicXorOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(15)).get();
        Thread.sleep(500);

        // atomicXor returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Long result = client.xor(key, hint, 7).get();
        assertNotNull(result);
        assertEquals(15, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(8, current.longValue()); // 15 ^ 7 = 8
    }

    @Test
    @DisplayName("atomicXor with double - INVALID_ARGUMENT on MASTER")
    void testAtomicXorDoubleInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(2.71)).get();
        Thread.sleep(500);

        // Bitwise XOR on double on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.xor(key, hint, 42).get();
            fail("Expected exception for bitwise on double");
        } catch (ExecutionException e) {
            // Expected
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
    }

    @Test
    @DisplayName("atomicXor on bytes - INVALID_ARGUMENT on MASTER")
    void testAtomicXorBytesInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_bytes_invalid_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // Bitwise XOR on bytes on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.xor(key, hint, 42).get();
            fail("Expected exception for bitwise on bytes");
        } catch (ExecutionException e) {
            // Expected
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 7. ATOMIC COMPARE AND SET (CAS) WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicCompareAndSet - success on MASTER, validate on BACKUP")
    void testAtomicCompareAndSetSuccessOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_success_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // CAS on MASTER - should succeed
        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, hint, 100, 200).get();
        assertNotNull(result);
        assertTrue(result.getResult());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(200, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicCompareAndSet - failure on MASTER, validate on BACKUP")
    void testAtomicCompareAndSetFailureOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_failure_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // CAS on MASTER - should fail
        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, hint, 999, 200).get();
        assertNotNull(result);
        assertFalse(result.getResult());
        Thread.sleep(500);

        // Verify on BACKUP - value should remain unchanged
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(100, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicCompareAndSet with double value on MASTER, validate on BACKUP")
    void testAtomicCompareAndSetDoubleOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        // CAS with double values on MASTER - should succeed
        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, hint, 3.14, 2.71).get();
        assertNotNull(result);
        assertTrue(result.getResult());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
        assertEquals(2.71, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicCompareAndSet with double - int expected returns INVALID_ARGUMENT on MASTER")
    void testAtomicCompareAndSetDoubleWithIntExpectedOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_double_int_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        // CAS with int expected on double value on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.compareAndSet(key, hint, 0L, 1L).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isDouble());
        assertEquals(3.14, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicCompareAndSet with bytes type - expecting long is INVALID_ARGUMENT on MASTER")
    void testAtomicCompareAndSetBytesInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_bytes_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // CAS with bytes on MASTER - expecting long is INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.compareAndSet(key, hint, 0L, 1L).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Value should remain unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertTrue(current.isBytes());
    }

    // =========================================================================
    // 8. ATOMIC OPERATIONS WITH LOCKS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicAdd with WRITE_LOCK on MASTER, validate on BACKUP")
    void testAtomicAddWithWriteLockOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_add_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // atomicAdd returns OLD value on MASTER
        Number result = client.add(key, hint, 5, 100).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint,100).get();
        assertEquals(15, current.longValue());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("atomicExchange with READ_LOCK on MASTER: denied for intruder")
    void testAtomicExchangeWithReadLockOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_read_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Intruder cannot perform atomic operations on BACKUP
        try {
            client.setMode(Mode.BACKUP).exchange(key, hint, 20, 999).get();
            fail("Expected exception for intruder");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("atomicCompareAndSet with WRITE_LOCK on MASTER, validate on BACKUP")
    void testAtomicCompareAndSetWithWriteLockOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_lock_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // CAS on MASTER - should succeed
        AtomicCasRes result = client.compareAndSet(key, hint, 100, 200, 100).get();
        assertNotNull(result);
        assertTrue(result.getResult());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint,100).get();
        assertEquals(200, current.longValue());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    // =========================================================================
    // 9. ATOMIC OPERATIONS TYPE SWITCHING WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicSub with double value - subtracting int is INVALID_ARGUMENT on MASTER")
    void testAtomicSubDoubleWithIntInvalidOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_double_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(50.0)).get();
        Thread.sleep(500);

        // atomicSub double with int on MASTER - should return INVALID_ARGUMENT
        client.setMode(Mode.MASTER);
        try {
            client.sub(key, hint, 1).get();
            fail("Expected exception for type mismatch");
        } catch (ExecutionException e) {
            // Expected - type mismatch
        }

        // Verify value remains unchanged on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(50.0, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicAdd after type switch bytes→int on MASTER, validate on BACKUP")
    void testAtomicAddAfterTypeSwitchBytesToIntOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "v" + UUID.randomUUID();

        // Create with bytes on MASTER
        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // Switch to int explicitly on MASTER
        client.setMode(Mode.MASTER);
        client.replace(key, hint, vi(50)).get();
        Thread.sleep(500);

        // atomicAdd int with int on MASTER - returns OLD value
        Number result = client.add(key, hint, 10).get();
        assertNotNull(result);
        assertEquals(50, result.longValue()); // Returns OLD value
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        Variant currentMaster = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals(60, currentMaster.longValue());
        assertEquals(60, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicSub after type switch int→double on MASTER, validate on BACKUP")
    void testAtomicSubAfterTypeSwitchIntToDoubleOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_type_switch_" + UUID.randomUUID();

        // Create with int on MASTER
        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // Switch to double explicitly on MASTER
        client.setMode(Mode.MASTER);
        client.replace(key, hint, vd(50.5)).get();
        Thread.sleep(500);

        // atomicSub double with double on MASTER - returns OLD value
        Number result = client.sub(key, hint, 5.5).get();
        assertNotNull(result);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(45.0, current.doubleValue(), 0.001);
    }

    // =========================================================================
    // 10. CHAINED ATOMIC OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("atomicAdd after type switch bytes→int on MASTER, validate on BACKUP")
    void testAtomicAddAfterTypeSwitchOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_type_switch_" + UUID.randomUUID();

        // Create with bytes
        KeyHintData hint = client.create(key, v("hello")).get();
        Thread.sleep(500);

        // Switch to int explicitly on MASTER
        client.setMode(Mode.MASTER);
        client.replace(key, hint, vi(50)).get();
        Thread.sleep(500);

        // atomicAdd int with int on MASTER - returns OLD value
        Number result = client.add(key, hint, 10).get();
        assertNotNull(result);
        assertEquals(50, result.longValue());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(60, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicSub after type switch int→double on MASTER, validate on BACKUP")
    void testAtomicSubAfterTypeSwitchOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_type_switch_" + UUID.randomUUID();

        // Create with int
        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // Switch to double explicitly on MASTER
        client.setMode(Mode.MASTER);
        client.replace(key, hint, vd(50.5)).get();
        Thread.sleep(500);

        // atomicSub double with double on MASTER - returns OLD value
        Number result = client.sub(key, hint, 5.5).get();
        assertNotNull(result);
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(45.0, current.doubleValue(), 0.001);
    }

    // =========================================================================
    // 10. CHAINED ATOMIC OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Multiple atomicAdd operations on MASTER, validate on BACKUP")
    void testMultipleAtomicAddOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_chained_add_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        // atomicAdd returns OLD value on MASTER
        client.setMode(Mode.MASTER);
        Number r1 = client.add(key, hint, 10).get();
        assertEquals(0, r1.longValue()); // Returns OLD value (0)

        Number r2 = client.add(key, hint, 20).get();
        assertEquals(10, r2.longValue()); // Returns OLD value (0+10=10)

        Number r3 = client.add(key, hint, 5).get();
        assertEquals(30, r3.longValue()); // Returns OLD value (10+20=30)
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(35, current.longValue()); // 0+10+20+5=35
    }

    @Test
    @DisplayName("Chained atomicAdd and atomicSub on MASTER, validate on BACKUP")
    void testChainedAddAndSubOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_chained_add_sub_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        // All operations on int type on MASTER
        client.setMode(Mode.MASTER);
        Number r1 = client.add(key, hint, 50).get();
        assertEquals(100, r1.longValue()); // OLD value

        Number r2 = client.sub(key, hint, 30).get();
        assertEquals(150, r2.longValue()); // OLD value

        Number r3 = client.add(key, hint, 10).get();
        assertEquals(120, r3.longValue()); // OLD value
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(130, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicExchange followed by atomicCompareAndSet on MASTER, validate on BACKUP")
    void testExchangeThenCasOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_cas_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        // Exchange on MASTER
        client.setMode(Mode.MASTER);
        Number prev = client.exchange(key, hint, 20).get();
        assertEquals(10, prev.longValue());
        Thread.sleep(500);

        // CAS should fail with wrong expected value
        AtomicCasRes cas = client.compareAndSet(key, hint, 100, 20).get();
        assertFalse(cas.getResult()); // 100 != 20, so should fail

        // Try correct expected value
        cas = client.compareAndSet(key, hint, 20, 30).get();
        assertTrue(cas.getResult());
        Thread.sleep(500);

        // Verify on BACKUP
        Variant current = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(30, current.longValue());
        assertTrue(current.isInt());
    }
}