package com.hurricache.client.intf;

import com.hurricache.grpc.AtomicCasRes;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientAtomics extends HurriCacheClientInterfaceCommon {

    /**
     * Atomically creates a 64-bit primitive scalar value.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    initial 64-bit value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing created {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> atomicCreate(byte[] key,
                                                KeyHintData hint,
                                                long value,
                                                Duration ttl,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<KeyHintData> atomicCreate(String key, KeyHintData hint, long value,
                                                        Duration ttl, int clientId, Duration timeout) {
        return atomicCreate(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, KeyHintData hint, long value) {
        return atomicCreate(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, KeyHintData hint, long value) {
        return atomicCreate(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, long value) {
        return atomicCreate(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, long value) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, long value, int clientId) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, long value, int clientId) {
        return atomicCreate(key, null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically overwrites a 64-bit scalar counter/value.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    new 64-bit value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing updated {@link KeyHintData}.
     */
    CompletableFuture<KeyHintData> atomicStore(byte[] key,
                                               KeyHintData hint,
                                               long value,
                                               Duration ttl,
                                               int clientId,
                                               Duration timeout);

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData hint, long value,
                                                       Duration ttl, int clientId, Duration timeout) {
        return atomicStore(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData hint, long value) {
        return atomicStore(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData hint, long value) {
        return atomicStore(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, long value) {
        return atomicStore(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, long value) {
        return atomicStore(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData keyHint, long value, int clientId) {
        return atomicStore(serializeKey(key), keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData keyHint, long value, int clientId) {
        return atomicStore(key, keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically sets a new 64-bit value and returns the old value.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param value    new 64-bit value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the previous value.
     */
    CompletableFuture<Long> atomicExchange(byte[] key,
                                           KeyHintData hint,
                                           long value,
                                           Duration ttl,
                                           int clientId,
                                           Duration timeout);

    default CompletableFuture<Long> atomicExchange(String key, KeyHintData hint, long value,
                                                   Duration ttl, int clientId, Duration timeout) {
        return atomicExchange(serializeKey(key), hint, value, ttl, clientId, timeout);
    }

    default CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value) {
        return atomicExchange(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(String key, KeyHintData hint, long value) {
        return atomicExchange(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(byte[] key, long value) {
        return atomicExchange(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(String key, long value) {
        return atomicExchange(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(String key, KeyHintData hint, long value, int clientId) {
        return atomicExchange(serializeKey(key), hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value, int clientId) {
        return atomicExchange(key, hint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically increments a 64-bit scalar value by a given delta.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param delta    increment value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the new value after increment.
     */
    CompletableFuture<Long> atomicAdd(byte[] key,
                                      KeyHintData hint,
                                      long delta,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicAdd(String key, KeyHintData hint, long delta,
                                              Duration ttl, int clientId, Duration timeout) {
        return atomicAdd(serializeKey(key), hint, delta, ttl, clientId, timeout);
    }

    default CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta) {
        return atomicAdd(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(String key, KeyHintData hint, long delta) {
        return atomicAdd(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(byte[] key, long delta) {
        return atomicAdd(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(String key, long delta) {
        return atomicAdd(serializeKey(key), null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(String key, KeyHintData hint, long delta, int clientId) {
        return atomicAdd(serializeKey(key), hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta, int clientId) {
        return atomicAdd(key, hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically decrements a 64-bit scalar value by a given delta.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param delta    decrement value.
     * @param ttl      container expiration duration.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the new value after decrement.
     */
    CompletableFuture<Long> atomicSub(byte[] key,
                                      KeyHintData hint,
                                      long delta,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicSub(String key, KeyHintData hint, long delta,
                                              Duration ttl, int clientId, Duration timeout) {
        return atomicSub(serializeKey(key), hint, delta, ttl, clientId, timeout);
    }

    default CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta) {
        return atomicSub(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(byte[] key, long delta) {
        return atomicSub(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(String key, KeyHintData hint, long delta) {
        return atomicSub(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(String key, long delta) {
        return atomicSub(serializeKey(key), null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(String key, KeyHintData hint, long delta, int clientId) {
        return atomicSub(serializeKey(key), hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta, int clientId) {
        return atomicSub(key, hint, delta, getDefaultTtl(), clientId, getDefaultTimeout());
    }

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
    CompletableFuture<Long> atomicAnd(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicAnd(String key, KeyHintData hint, long mask,
                                              Duration ttl, int clientId, Duration timeout) {
        return atomicAnd(serializeKey(key), hint, mask, ttl, clientId, timeout);
    }

    default CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask) {
        return atomicAnd(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(String key, KeyHintData hint, long mask) {
        return atomicAnd(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(byte[] key, long mask) {
        return atomicAnd(key, null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(String key, long mask) {
        return atomicAnd(serializeKey(key), null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(String key, KeyHintData hint, long mask, int clientId) {
        return atomicAnd(serializeKey(key), hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask, int clientId) {
        return atomicAnd(key, hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

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
    CompletableFuture<Long> atomicOr(byte[] key,
                                     KeyHintData hint,
                                     long mask,
                                     Duration ttl,
                                     int clientId,
                                     Duration timeout);

    default CompletableFuture<Long> atomicOr(String key, KeyHintData hint, long mask,
                                             Duration ttl, int clientId, Duration timeout) {
        return atomicOr(serializeKey(key), hint, mask, ttl, clientId, timeout);
    }

    default CompletableFuture<Long> atomicOr(byte[] key, KeyHintData hint, long mask) {
        return atomicOr(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(String key, KeyHintData hint, long mask) {
        return atomicOr(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(byte[] key, long mask) {
        return atomicOr(key, null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(String key, long mask) {
        return atomicOr(serializeKey(key), null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(String key, KeyHintData keyHint, long mask, int clientId) {
        return atomicOr(serializeKey(key), keyHint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(byte[] key, KeyHintData keyHint, long mask, int clientId) {
        return atomicOr(key, keyHint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

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
    CompletableFuture<Long> atomicXor(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicXor(String key, KeyHintData hint, long mask,
                                              Duration ttl, int clientId, Duration timeout) {
        return atomicXor(serializeKey(key), hint, mask, ttl, clientId, timeout);
    }

    default CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask) {
        return atomicXor(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(String key, KeyHintData hint, long mask) {
        return atomicXor(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(byte[] key, long mask) {
        return atomicXor(key, null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(String key, long mask) {
        return atomicXor(serializeKey(key), null, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(String key, KeyHintData hint, long mask, int clientId) {
        return atomicXor(serializeKey(key), hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask, int clientId) {
        return atomicXor(key, hint, mask, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Performs an atomic Compare-And-Swap (CAS) operation on a primitive 64-bit scalar.
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
    CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                        KeyHintData hint,
                                                        long expectedValue,
                                                        long newValue,
                                                        Duration ttl,
                                                        int clientId,
                                                        Duration timeout);

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key, KeyHintData hint,
                                                                long expectedValue, long newValue,
                                                                Duration ttl, int clientId, Duration timeout) {
        return atomicCompareAndSet(serializeKey(key), hint, expectedValue, newValue, ttl, clientId, timeout);
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHintData keyHint,
                                                                long expectedValue, long newValue) {
        return atomicCompareAndSet(key, keyHint, expectedValue, newValue,
                                   getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key, KeyHintData keyHint,
                                                                long expectedValue, long newValue) {
        return atomicCompareAndSet(serializeKey(key), keyHint, expectedValue, newValue,
                                   getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, long expectedValue, long newValue) {
        return atomicCompareAndSet(key, null, expectedValue, newValue,
                                   getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key, long expectedValue, long newValue) {
        return atomicCompareAndSet(serializeKey(key), null, expectedValue, newValue,
                                   getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key, KeyHintData hint,
                                                                long expectedValue, long newValue,
                                                                int clientId) {
        return atomicCompareAndSet(serializeKey(key), hint, expectedValue, newValue,
                                   getDefaultTtl(), clientId, getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHintData hint,
                                                                long expectedValue, long newValue,
                                                                int clientId) {
        return atomicCompareAndSet(key, hint, expectedValue, newValue,
                                   getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Loads current value of an atomic scalar.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the current scalar value.
     */
    CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicLoad(String key, KeyHintData hint, int clientId, Duration timeout) {
        return atomicLoad(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Long> atomicLoad(String key) {
        return atomicLoad(key, getDefaultClientId());
    }

    default CompletableFuture<Long> atomicLoad(byte[] key) {
        return atomicLoad(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(String key, int clientId) {
        return atomicLoad(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(byte[] key, int clientId) {
        return atomicLoad(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(String key, KeyHintData hint) {
        return atomicLoad(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint) {
        return atomicLoad(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Reads current value of an atomic scalar and immediately removes it.
     *
     * @param key      target key in byte array form.
     * @param hint     optional key routing hint.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the read scalar value.
     */
    CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicLoadAndDelete(String key, KeyHintData hint, int clientId, Duration timeout) {
        return atomicLoadAndDelete(serializeKey(key), hint, clientId, timeout);
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key) {
        return atomicLoadAndDelete(key, getDefaultClientId());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(byte[] key) {
        return atomicLoadAndDelete(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key, int clientId) {
        return atomicLoadAndDelete(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(byte[] key, int clientId) {
        return atomicLoadAndDelete(key, null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key, KeyHintData hint) {
        return atomicLoadAndDelete(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint) {
        return atomicLoadAndDelete(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
}