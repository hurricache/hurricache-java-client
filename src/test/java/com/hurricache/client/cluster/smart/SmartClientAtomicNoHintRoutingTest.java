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
 * SmartClient routing tests for Atomic operations WITHOUT keyHint.
 */
public class SmartClientAtomicNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("atomicExchange with int value on MASTER without keyHint")
    void testAtomicExchangeIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_int_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number previous = client.exchange(key, 200).get();
        assertNotNull(previous);
        assertEquals(100, previous.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(200, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicExchange with double value on MASTER without keyHint")
    void testAtomicExchangeDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_double_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number previous = client.exchange(key, 2.71).get();
        assertNotNull(previous);
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertTrue(current.isDouble());
        assertEquals(2.71, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicAdd with int value on MASTER without keyHint")
    void testAtomicAddIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_add_int_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number result = client.add(key, 5).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(15, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicAdd with negative delta on MASTER without keyHint")
    void testAtomicAddNegativeWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_add_negative_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number result = client.add(key, -3).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(7, current.longValue());
    }

    @Test
    @DisplayName("atomicAdd with double value on MASTER without keyHint")
    void testAtomicAddDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_add_double_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(10.5)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number result = client.add(key, 2.5).get();
        assertNotNull(result);
        Thread.sleep(500);

        Variant currentB = client.setMode(Mode.BACKUP).get(key).get();
        Variant currentM = client.setMode(Mode.MASTER).get(key).get();
        assertNotNull(currentM);
        assertTrue(currentM.isDouble());
        assertEquals(13.0, currentM.doubleValue(), 0.001);
        assertNotNull(currentB);
        assertTrue(currentB.isDouble());
        assertEquals(13.0, currentB.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicSub with int value on MASTER without keyHint")
    void testAtomicSubIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_int_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number result = client.atomicSub(key, 25).get();
        assertNotNull(result);
        assertEquals(100, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(75, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicSub with double value on MASTER without keyHint")
    void testAtomicSubDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_double_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(50.0)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number result = client.atomicSub(key, 5.5).get();
        assertNotNull(result);
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertNotNull(current);
        assertTrue(current.isDouble());
        assertEquals(44.5, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicSub goes negative on MASTER without keyHint")
    void testAtomicSubNegativeWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_sub_negative_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number result = client.atomicSub(key, 30).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(-20, current.longValue());
    }

    @Test
    @DisplayName("atomicAnd with int value on MASTER without keyHint")
    void testAtomicAndWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_and_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(255)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long result = client.and(key, 15).get();
        assertNotNull(result);
        assertEquals(255, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(15, current.longValue());
    }

    @Test
    @DisplayName("atomicOr with int value on MASTER without keyHint")
    void testAtomicOrWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_or_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(8)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long result = client.or(key, 7).get();
        assertNotNull(result);
        assertEquals(8, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(15, current.longValue());
    }

    @Test
    @DisplayName("atomicXor with int value on MASTER without keyHint")
    void testAtomicXorWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_xor_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(15)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Long result = client.xor(key, 7).get();
        assertNotNull(result);
        assertEquals(15, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(8, current.longValue());
    }

    @Test
    @DisplayName("atomicCompareAndSet - success on MASTER without keyHint")
    void testAtomicCompareAndSetSuccessWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_success_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, 100, 200).get();
        assertNotNull(result);
        assertTrue(result.getResult());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(200, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicCompareAndSet - failure on MASTER without keyHint")
    void testAtomicCompareAndSetFailureWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_failure_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, 999, 200).get();
        assertNotNull(result);
        assertFalse(result.getResult());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(100, current.longValue());
        assertTrue(current.isInt());
    }

    @Test
    @DisplayName("atomicCompareAndSet with double value on MASTER without keyHint")
    void testAtomicCompareAndSetDoubleWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_cas_double_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vd(3.14)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, 3.14, 2.71).get();
        assertNotNull(result);
        assertTrue(result.getResult());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertTrue(current.isDouble());
        assertEquals(2.71, current.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("atomicAdd with WRITE_LOCK on MASTER without keyHint")
    void testAtomicAddWithWriteLockWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_add_lock_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        Number result = client.add(key, 5).get();
        assertNotNull(result);
        assertEquals(10, result.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(15, current.longValue());

        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("atomicExchange with READ_LOCK on MASTER without keyHint")
    void testAtomicExchangeWithReadLockWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_exchange_read_lock_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(10)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        try {
            client.setMode(Mode.BACKUP).exchange(key, 20).get();
            fail("Expected exception for intruder");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("Multiple atomicAdd operations on MASTER without keyHint")
    void testMultipleAtomicAddWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_chained_add_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number r1 = client.add(key, 10).get();
        assertEquals(0, r1.longValue());

        Number r2 = client.add(key, 20).get();
        assertEquals(10, r2.longValue());

        Number r3 = client.add(key, 5).get();
        assertEquals(30, r3.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(35, current.longValue());
    }

    @Test
    @DisplayName("Chained atomicAdd and atomicSub on MASTER without keyHint")
    void testChainedAddAndSubWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "atomic_chained_add_sub_no_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Number r1 = client.add(key, 50).get();
        assertEquals(100, r1.longValue());

        Number r2 = client.atomicSub(key, 30).get();
        assertEquals(150, r2.longValue());

        Number r3 = client.add(key, 10).get();
        assertEquals(120, r3.longValue());
        Thread.sleep(500);

        Variant current = client.setMode(Mode.BACKUP).get(key).get();
        assertEquals(130, current.longValue());
        assertTrue(current.isInt());
    }
}