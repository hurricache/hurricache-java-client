package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Overflow/underflow protection for atomic arithmetic and failed CAS semantics.
 */
public class AtomicOverflowTest extends TestBase {

    private static void assertStatusCode(CompletableFuture<?> future, Status.Code expected) {
        try {
            future.get();
            fail("Expected " + expected);
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(expected, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    // =========================================================================
    // 1. INT64 OVERFLOW / UNDERFLOW
    // =========================================================================

    @Test
    @DisplayName("atomicAdd on Long.MAX_VALUE - OUT_OF_RANGE, value unchanged")
    void testAtomicAddLongMaxOverflow() throws ExecutionException, InterruptedException {
        String key = "atomic_overflow_add_max_" + UUID.randomUUID();

        client.create(key, Variant.ofLong(Long.MAX_VALUE)).get();

        assertStatusCode(client.add(key, 1), Status.Code.OUT_OF_RANGE);

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isInt());
        assertEquals(Long.MAX_VALUE, current.longValue());
    }

    @Test
    @DisplayName("atomicSub on Long.MIN_VALUE - OUT_OF_RANGE, value unchanged")
    void testAtomicSubLongMinUnderflow() throws ExecutionException, InterruptedException {
        String key = "atomic_overflow_sub_min_" + UUID.randomUUID();

        client.create(key, Variant.ofLong(Long.MIN_VALUE)).get();

        assertStatusCode(client.sub(key, 1), Status.Code.OUT_OF_RANGE);

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isInt());
        assertEquals(Long.MIN_VALUE, current.longValue());
    }

    @Test
    @DisplayName("atomicSub on Long.MIN_VALUE with delta -1 - allowed, result is MIN_VALUE + 1")
    void testAtomicSubLongMinNegativeDelta() throws ExecutionException, InterruptedException {
        String key = "atomic_overflow_sub_min_neg_" + UUID.randomUUID();

        client.create(key, Variant.ofLong(Long.MIN_VALUE)).get();

        // Server semantics: sub with delta -1 on MIN_VALUE does NOT underflow
        // (MIN_VALUE - (-1) = MIN_VALUE + 1 fits in int64)
        Number old = client.sub(key, -1).get();
        assertNotNull(old);
        assertEquals(Long.MIN_VALUE, old.longValue()); // Returns OLD value

        Variant current = client.get(key).get();
        assertEquals(Long.MIN_VALUE + 1, current.longValue());
    }

    // =========================================================================
    // 2. DOUBLE OVERFLOW
    // =========================================================================

    @Test
    @DisplayName("atomicAdd Double.MAX_VALUE + Double.MAX_VALUE - OUT_OF_RANGE, value unchanged")
    void testAtomicAddDoubleMaxOverflow() throws ExecutionException, InterruptedException {
        String key = "atomic_overflow_add_double_max_" + UUID.randomUUID();

        client.create(key, vd(Double.MAX_VALUE)).get();

        // Server checks double overflow too (not only int)
        assertStatusCode(client.add(key, Double.MAX_VALUE), Status.Code.OUT_OF_RANGE);

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
        assertEquals(Double.MAX_VALUE, current.doubleValue());
    }

    // =========================================================================
    // 3. FAILED CAS RETURNS ACTUAL VALUE
    // =========================================================================

    @Test
    @DisplayName("compareAndSet failure on int - result=false, response holds current value, value unchanged")
    void testCasFailureIntReturnsCurrentValue() throws ExecutionException, InterruptedException {
        String key = "atomic_overflow_cas_fail_int_" + UUID.randomUUID();

        client.create(key, vi(42)).get();

        AtomicCasRes result = client.compareAndSet(key, 999, 1000).get();
        assertNotNull(result);
        assertFalse(result.getResult());

        // Response contains the actual current value
        assertTrue(result.hasExpected());
        assertEquals(42, result.getExpected().getValueUnordered().getValue().getIntPayload());

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isInt());
        assertEquals(42, current.longValue());
    }

    @Test
    @DisplayName("compareAndSet failure on double - result=false, response holds current value, value unchanged")
    void testCasFailureDoubleReturnsCurrentValue() throws ExecutionException, InterruptedException {
        String key = "atomic_overflow_cas_fail_double_" + UUID.randomUUID();

        client.create(key, vd(3.14)).get();

        AtomicCasRes result = client.compareAndSet(key, 9.9, 10.1).get();
        assertNotNull(result);
        assertFalse(result.getResult());

        // Response contains the actual current value
        assertTrue(result.hasExpected());
        assertEquals(3.14, result.getExpected().getValueUnordered().getValue().getDoublePayload(), 0.001);

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isDouble());
        assertEquals(3.14, current.doubleValue(), 0.001);
    }
}
