package com.hurricache.client.intf;

import com.hurricache.grpc.AtomicCasRes;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Atomic operations interface for HurriCache SmartClient.
 * 
 * All methods follow a unified signature pattern:
 * <ul>
 *   <li>Full signature: {@code method(byte[]|String key, KeyHintData hint, value, Duration ttl, int clientId, Duration timeout)}</li>
 *   <li>Minimal signature: {@code method(String key, KeyHintData hint, value)} — uses defaults for ttl, clientId, timeout</li>
 *   <li>With clientId: {@code method(String|byte[] key, KeyHintData hint, value, int clientId)} — uses defaults for ttl, timeout</li>
 * </ul>
 * 
 * Default values:
 * <ul>
 *   <li>ttl → {@link #getDefaultTtl()}</li>
 *   <li>clientId → {@link #getDefaultClientId()}</li>
 *   <li>timeout → {@link #getDefaultTimeout()}</li>
 * </ul>
 */
public interface HurriCacheClientAtomics extends HurriCacheClientInterfaceCommon {

    // =========================================================================
    // EXCHANGE — Atomically sets a new value and returns the old value
    // =========================================================================

    /**
     * Atomically sets a new 64-bit value and returns the old value.
     * Can return int or double depending on the stored ValueType.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    new long value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the previous value as Number.
     */
    CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout);

    /**
     * Atomically sets a new double value and returns the old value.
     * Can return int or double depending on the stored ValueType.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    new double value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the previous value as Number.
     */
    CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, double value, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for exchange(long) */
    default CompletableFuture<Number> exchange(String key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return exchange(serializeKey(key), hint, value, ttl, clientId, timeout);
    }
    default CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, long value) {
        return exchange(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, KeyHintData hint, long value) {
        return exchange(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(byte[] key, long value) {
        return exchange(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, long value) {
        return exchange(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, long value, int clientId) {
        return exchange(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(byte[] key, long value, int clientId) {
        return exchange(key, null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, KeyHintData hint, long value, int clientId) {
        return exchange(serializeKey(key), hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, long value, int clientId) {
        return exchange(key, hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /** Default overloads for exchange(double) */
    default CompletableFuture<Number> exchange(String key, KeyHintData hint, double value, Duration ttl, int clientId, Duration timeout) {
        return exchange(serializeKey(key), hint, value, ttl, clientId, timeout);
    }
    default CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, double value) {
        return exchange(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, KeyHintData hint, double value) {
        return exchange(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(byte[] key, double value) {
        return exchange(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, double value) {
        return exchange(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, double value, int clientId) {
        return exchange(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(byte[] key, double value, int clientId) {
        return exchange(key, null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(String key, KeyHintData hint, double value, int clientId) {
        return exchange(serializeKey(key), hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, double value, int clientId) {
        return exchange(key, hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    // =========================================================================
    // ADD — Atomically increments a scalar value by a given delta
    // =========================================================================

    /**
     * Atomically increments a 64-bit scalar value by a given delta.
     * Can return int or double depending on the stored ValueType.
     *
     * @param key   target key in byte array form.
     * @param hint  optional key routing hint.
     * @param delta increment value.
     * @param ttl   container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout execution timeout duration.
     * @return a {@link CompletableFuture} containing the new value as Number.
     */
    CompletableFuture<Number> add(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout);

    /**
     * Atomically increments a double scalar value by a given delta.
     * Can return int or double depending on the stored ValueType.
     *
     * @param key   target key in byte array form.
     * @param hint  optional key routing hint.
     * @param delta increment double value.
     * @param ttl   container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout execution timeout duration.
     * @return a {@link CompletableFuture} containing the new value as Number.
     */
    CompletableFuture<Number> add(byte[] key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for add(long) */
    default CompletableFuture<Number> add(String key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return add(serializeKey(key), hint, delta, ttl, clientId, timeout);
    }
    default CompletableFuture<Number> add(byte[] key, KeyHintData hint, long delta) {
        return add(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, KeyHintData hint, long delta) {
        return add(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(byte[] key, long delta) {
        return add(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, long delta) {
        return add(serializeKey(key), null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, long delta, int clientId) {
        return add(serializeKey(key), null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> add(byte[] key, long delta, int clientId) {
        return add(key, null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, KeyHintData hint, long delta, int clientId) {
        return add(serializeKey(key), hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> add(byte[] key, KeyHintData hint, long delta, int clientId) {
        return add(key, hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /** Default overloads for add(double) */
    default CompletableFuture<Number> add(String key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout) {
        return add(serializeKey(key), hint, delta, ttl, clientId, timeout);
    }
    default CompletableFuture<Number> add(byte[] key, KeyHintData hint, double delta) {
        return add(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, KeyHintData hint, double delta) {
        return add(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(byte[] key, double delta) {
        return add(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, double delta) {
        return add(serializeKey(key), null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, double delta, int clientId) {
        return add(serializeKey(key), null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> add(byte[] key, double delta, int clientId) {
        return add(key, null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> add(String key, KeyHintData hint, double delta, int clientId) {
        return add(serializeKey(key), hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> add(byte[] key, KeyHintData hint, double delta, int clientId) {
        return add(key, hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    // =========================================================================
    // ATOMIC SUB — Atomically decrements a scalar value by a given delta
    // =========================================================================

    /**
     * Atomically decrements a 64-bit scalar value by a given delta.
     * Can return int or double depending on the stored ValueType.
     *
     * @param key   target key in byte array form.
     * @param hint  optional key routing hint.
     * @param delta decrement value.
     * @param ttl   container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout execution timeout duration.
     * @return a {@link CompletableFuture} containing the new value as Number.
     */
    CompletableFuture<Number> sub(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout);

    /**
     * Atomically decrements a double scalar value by a given delta.
     * Can return int or double depending on the stored ValueType.
     *
     * @param key   target key in byte array form.
     * @param hint  optional key routing hint.
     * @param delta decrement double value.
     * @param ttl   container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout execution timeout duration.
     * @return a {@link CompletableFuture} containing the new value as Number.
     */
    CompletableFuture<Number> sub(byte[] key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for atomicSub(long) */
    default CompletableFuture<Number> sub(String key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return sub(serializeKey(key), hint, delta, ttl, clientId, timeout);
    }
    default CompletableFuture<Number> sub(byte[] key, KeyHintData hint, long delta) {
        return sub(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, KeyHintData hint, long delta) {
        return sub(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(byte[] key, long delta) {
        return sub(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, long delta) {
        return sub(serializeKey(key), null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, long delta, int clientId) {
        return sub(serializeKey(key), null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(byte[] key, long delta, int clientId) {
        return sub(key, null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, KeyHintData hint, long delta, int clientId) {
        return sub(serializeKey(key), hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(byte[] key, KeyHintData hint, long delta, int clientId) {
        return sub(key, hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /** Default overloads for atomicSub(double) */
    default CompletableFuture<Number> sub(String key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout) {
        return sub(serializeKey(key), hint, delta, ttl, clientId, timeout);
    }
    default CompletableFuture<Number> sub(byte[] key, KeyHintData hint, double delta) {
        return sub(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, KeyHintData hint, double delta) {
        return sub(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(byte[] key, double delta) {
        return sub(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, double delta) {
        return sub(serializeKey(key), null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, double delta, int clientId) {
        return sub(serializeKey(key), null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(byte[] key, double delta, int clientId) {
        return sub(key, null, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(String key, KeyHintData hint, double delta, int clientId) {
        return sub(serializeKey(key), hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Number> sub(byte[] key, KeyHintData hint, double delta, int clientId) {
        return sub(key, hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    // =========================================================================
    // AND — Atomic bitwise AND mask operation
    // =========================================================================

    /**
     * Applies an atomic bitwise AND mask operation on a scalar value.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param mask     bitmask to apply.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the resulting value after AND.
     */
    CompletableFuture<Long> and(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for and(long) */
    default CompletableFuture<Long> and(String key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return and(serializeKey(key), hint, mask, ttl, clientId, timeout);
    }
    default CompletableFuture<Long> and(byte[] key, KeyHintData hint, long mask) {
        return and(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> and(String key, KeyHintData hint, long mask) {
        return and(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> and(byte[] key, long mask) {
        return and(key, null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> and(String key, long mask) {
        return and(serializeKey(key), null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> and(String key, KeyHintData hint, long mask, int clientId) {
        return and(serializeKey(key), hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Long> and(byte[] key, KeyHintData hint, long mask, int clientId) {
        return and(key, hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    // =========================================================================
    // OR — Atomic bitwise OR mask operation
    // =========================================================================

    /**
     * Applies an atomic bitwise OR mask operation on a scalar value.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param mask     bitmask to apply.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the resulting value after OR.
     */
    CompletableFuture<Long> or(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for or(long) */
    default CompletableFuture<Long> or(String key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return or(serializeKey(key), hint, mask, ttl, clientId, timeout);
    }
    default CompletableFuture<Long> or(byte[] key, KeyHintData hint, long mask) {
        return or(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> or(String key, KeyHintData hint, long mask) {
        return or(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> or(byte[] key, long mask) {
        return or(key, null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> or(String key, long mask) {
        return or(serializeKey(key), null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> or(String key, KeyHintData hint, long mask, int clientId) {
        return or(serializeKey(key), hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Long> or(byte[] key, KeyHintData hint, long mask, int clientId) {
        return or(key, hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    // =========================================================================
    // XOR — Atomic bitwise XOR mask operation
    // =========================================================================

    /**
     * Applies an atomic bitwise XOR mask operation on a scalar value.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param mask     bitmask to apply.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the resulting value after XOR.
     */
    CompletableFuture<Long> xor(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for xor(long) */
    default CompletableFuture<Long> xor(String key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return xor(serializeKey(key), hint, mask, ttl, clientId, timeout);
    }
    default CompletableFuture<Long> xor(byte[] key, KeyHintData hint, long mask) {
        return xor(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> xor(String key, KeyHintData hint, long mask) {
        return xor(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> xor(byte[] key, long mask) {
        return xor(key, null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> xor(String key, long mask) {
        return xor(serializeKey(key), null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Long> xor(String key, KeyHintData hint, long mask, int clientId) {
        return xor(serializeKey(key), hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<Long> xor(byte[] key, KeyHintData hint, long mask, int clientId) {
        return xor(key, hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    // =========================================================================
    // COMPARE AND SET — Atomic CAS operation on a primitive scalar
    // =========================================================================

    /**
     * Performs an atomic Compare-And-Swap (CAS) operation on a 64-bit scalar.
     *
     * @param key           target key in byte array form.
     * @param hint          optional key routing hint.
     * @param expectedValue expected existing scalar value.
     * @param newValue      new target value to apply if match succeeds.
     * @param ttl           container expiration duration.
     * @param clientId      identifier of the issuing client.
     * @param timeout       execution timeout duration.
     * @return resulting {@link AtomicCasRes} indicating success/failure and current value.
     */
    CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout);

    /**
     * Performs an atomic Compare-And-Swap (CAS) operation on a double scalar.
     *
     * @param key           target key in byte array form.
     * @param hint          optional key routing hint.
     * @param expectedValue expected existing double value.
     * @param newValue      new target double value to apply if match succeeds.
     * @param ttl           container expiration duration.
     * @param clientId      identifier of the issuing client.
     * @param timeout       execution timeout duration.
     * @return resulting {@link AtomicCasRes} indicating success/failure and current value.
     */
    CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, double expectedValue, double newValue, Duration ttl, int clientId, Duration timeout);

    /** Default overloads for compareAndSet(long) */
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, KeyHintData hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout) {
        return compareAndSet(serializeKey(key), hint, expectedValue, newValue, ttl, clientId, timeout);
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue) {
        return compareAndSet(key, hint, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, KeyHintData hint, long expectedValue, long newValue) {
        return compareAndSet(serializeKey(key), hint, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, long expectedValue, long newValue) {
        return compareAndSet(key, null, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, long expectedValue, long newValue) {
        return compareAndSet(serializeKey(key), null, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, KeyHintData hint, long expectedValue, long newValue, int clientId) {
        return compareAndSet(serializeKey(key), hint, expectedValue, newValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue, int clientId) {
        return compareAndSet(key, hint, expectedValue, newValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /** Default overloads for compareAndSet(double) */
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, KeyHintData hint, double expectedValue, double newValue, Duration ttl, int clientId, Duration timeout) {
        return compareAndSet(serializeKey(key), hint, expectedValue, newValue, ttl, clientId, timeout);
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, double expectedValue, double newValue) {
        return compareAndSet(key, hint, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, KeyHintData hint, double expectedValue, double newValue) {
        return compareAndSet(serializeKey(key), hint, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, double expectedValue, double newValue) {
        return compareAndSet(key, null, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, double expectedValue, double newValue) {
        return compareAndSet(serializeKey(key), null, expectedValue, newValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(String key, KeyHintData hint, double expectedValue, double newValue, int clientId) {
        return compareAndSet(serializeKey(key), hint, expectedValue, newValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }
    default CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, double expectedValue, double newValue, int clientId) {
        return compareAndSet(key, hint, expectedValue, newValue, getDefaultTtl(), clientId, getDefaultTimeout());
    }
}