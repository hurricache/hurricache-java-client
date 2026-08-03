package com.hurricache.client.intf;

import com.hurricache.grpc.AtomicCasRes;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientAtomics extends HurriCacheClientInterfaceCommon {

    /**
     * Atomically creates a 64-bit primitive scalar value.
     */
    CompletableFuture<KeyHintData> atomicCreate(byte[] key,
                                                KeyHintData hint,
                                                long value,
                                                Duration ttl,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, KeyHintData hint, long value) {
        return atomicCreate(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, KeyHintData hint, long value) {
        return atomicCreate(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, long value) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(byte[] key, long value) {
        return atomicCreate(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicCreate(String key, long value, int clientId) {
        return atomicCreate(serializeKey(key), null, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically overwrites a 64-bit scalar counter/value.
     */
    CompletableFuture<KeyHintData> atomicStore(byte[] key,
                                               KeyHintData hint,
                                               long value,
                                               Duration ttl,
                                               int clientId,
                                               Duration timeout);

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData hint, long value) {
        return atomicStore(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData hint, long value) {
        return atomicStore(serializeKey(key), hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(byte[] key, long value) {
        return atomicStore(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<KeyHintData> atomicStore(String key, KeyHintData keyHint, long value, int clientId) {
        return atomicStore(serializeKey(key), keyHint, value, getDefaultTtl(), clientId, getDefaultTimeout());
    }

    /**
     * Atomically sets a new 64-bit value and returns the old value.
     */
    CompletableFuture<Long> atomicExchange(byte[] key,
                                           KeyHintData hint,
                                           long value,
                                           Duration ttl,
                                           int clientId,
                                           Duration timeout);

    default CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value) {
        return atomicExchange(key, hint, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(String key, KeyHintData hint, long value) {
        return atomicExchange(serializeKey(key),
                              hint,
                              value,
                              getDefaultTtl(),
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicExchange(byte[] key, long value) {
        return atomicExchange(key, null, value, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically increments a 64-bit scalar value by a given delta.
     */
    CompletableFuture<Long> atomicAdd(byte[] key,
                                      KeyHintData hint,
                                      long delta,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta) {
        return atomicAdd(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(String key, KeyHintData hint, long delta) {
        return atomicAdd(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAdd(byte[] key, long delta) {
        return atomicAdd(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically decrements a 64-bit scalar value by a given delta.
     */
    CompletableFuture<Long> atomicSub(byte[] key,
                                      KeyHintData hint,
                                      long delta,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta) {
        return atomicSub(key, hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(byte[] key, long delta) {
        return atomicSub(key, null, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicSub(String key, KeyHintData hint, long delta) {
        return atomicSub(serializeKey(key), hint, delta, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Applies an atomic bitwise AND mask operation on a scalar value.
     */
    CompletableFuture<Long> atomicAnd(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask) {
        return atomicAnd(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicAnd(String key, KeyHintData hint, long mask) {
        return atomicAnd(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Applies an atomic bitwise OR mask operation on a scalar value.
     */
    CompletableFuture<Long> atomicOr(byte[] key,
                                     KeyHintData hint,
                                     long mask,
                                     Duration ttl,
                                     int clientId,
                                     Duration timeout);

    default CompletableFuture<Long> atomicOr(byte[] key, KeyHintData hint, long mask) {
        return atomicOr(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicOr(String key, KeyHintData keyHint, long mask) {
        return atomicOr(serializeKey(key), keyHint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Applies an atomic bitwise XOR mask operation on a scalar value.
     */
    CompletableFuture<Long> atomicXor(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout);

    default CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask) {
        return atomicXor(key, hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicXor(String key, KeyHintData hint, long mask) {
        return atomicXor(serializeKey(key), hint, mask, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Performs an atomic Compare-And-Swap (CAS) operation on a primitive 64-bit scalar.
     *
     * @param expectedValue expected existing scalar value.
     * @param newValue      new target value to apply if match succeeds.
     * @return resulting {@link AtomicCasRes} indicating success/failure and current value.
     */
    CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                        KeyHintData hint,
                                                        long expectedValue,
                                                        long newValue,
                                                        Duration ttl,
                                                        int clientId,
                                                        Duration timeout);

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                                KeyHintData keyHint,
                                                                long expectedValue,
                                                                long newValue) {
        return atomicCompareAndSet(key,
                                   keyHint,
                                   expectedValue,
                                   newValue,
                                   getDefaultTtl(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key,
                                                                KeyHintData keyHint,
                                                                long expectedValue,
                                                                long newValue) {
        return atomicCompareAndSet(serializeKey(key),
                                   keyHint,
                                   expectedValue,
                                   newValue,
                                   getDefaultTtl(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<AtomicCasRes> atomicCompareAndSet(String key,
                                                                KeyHintData hint,
                                                                long expectedValue,
                                                                long newValue,
                                                                int clientId) {
        return atomicCompareAndSet(serializeKey(key),
                                   hint,
                                   expectedValue,
                                   newValue,
                                   getDefaultTtl(),
                                   clientId,
                                   getDefaultTimeout());
    }

    /**
     * Loads current value of an atomic scalar.
     */
    CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicLoad(String key) {
        return atomicLoad(key, getDefaultClientId());
    }

    default CompletableFuture<Long> atomicLoad(String key, int clientId) {
        return atomicLoad(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(String key, KeyHintData hint) {
        return atomicLoad(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint) {
        return atomicLoad(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Reads current value of an atomic scalar and immediately removes it.
     */
    CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Long> atomicLoadAndDelete(String key) {
        return atomicLoadAndDelete(key, getDefaultClientId());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key, int clientId) {
        return atomicLoadAndDelete(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(String key, KeyHintData hint) {
        return atomicLoadAndDelete(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint) {
        return atomicLoadAndDelete(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
}