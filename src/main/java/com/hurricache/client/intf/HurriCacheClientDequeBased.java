package com.hurricache.client.intf;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientDequeBased extends HurriCacheClientInterfaceCommon{
    /**
     * Atomically retrieves and removes the front element of a container (Pop Front / Dequeue).
     */
    CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getAndRemoveFront(String key) {
        return getAndRemoveFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key, KeyHintData hint) {
        return getAndRemoveFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(String key, int clientId) {
        return getAndRemoveFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint) {
        return getAndRemoveFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }


    /**
     * Reads the front element of a container without modifying structure (Peek Front).
     */
    CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getFront(String key) {
        return getFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(byte[] key) {
        return getFront(key, null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(String key, KeyHintData hint) {
        return getFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(String key, KeyHintData hint, int clientId) {
        return getFront(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(String key, int clientId) {
        return getFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint) {
        return getFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Gets the element at the head of a sequence.
     */
    CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getHead(String key) {
        return getHead(key, getDefaultClientId());
    }

    default CompletableFuture<Payload> getHead(String key, int clientId) {
        return getHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(String key, KeyHintData hint) {
        return getHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Gets the element at the tail of a sequence.
     */
    CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getTail(String key) {
        return getTail(key, getDefaultClientId());
    }

    default CompletableFuture<Payload> getTail(String key, KeyHintData hint) {
        return getTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint) {
        return getTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Atomically retrieves and removes the tail element of a container (Pop Back).
     */
    CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Payload> getAndRemoveTail(String key) {
        return getAndRemoveTail(key, getDefaultClientId());
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key, KeyHintData hint) {
        return getAndRemoveTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint) {
        return getAndRemoveTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveTail(String key, int clientId) {
        return getAndRemoveTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    /**
     * Appends a collection of elements to the tail of a container (Push Back).
     */
    CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                KeyHintData hint,
                                                List<Payload> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToTail(String key, KeyHintData hint, List<Payload> data) {
        return addElementToTail(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(String key,
                                                        KeyHintData hint,
                                                        List<Payload> data,
                                                        int clientId) {
        return addElementToTail(serializeKey(key), hint, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Prepends a collection of elements to the head of a container (Push Front).
     */
    CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                KeyHintData hint,
                                                List<Payload> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToHead(String key, List<Payload> data) {
        return addElementToHead(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToHead(String key, KeyHintData hint, List<Payload> data) {
        return addElementToHead(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToHead(byte[] key, KeyHintData keyHint, List<Payload> data) {
        return addElementToHead(key, keyHint, data, getDefaultClientId(), getDefaultTimeout());
    }
    /**
     * Removes the tail element of a sequence without returning the deleted element.
     */
    CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeTail(String key, int clientId) {
        return removeTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, KeyHintData hint) {
        return removeTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint) {
        return removeTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Removes the head element of a sequence without returning the deleted element.
     */
    CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeHead(String key, int clientId) {
        return removeHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint) {
        return removeHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(String key, KeyHintData hint) {
        return removeHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }
}