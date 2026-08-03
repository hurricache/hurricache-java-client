package com.hurricache.client.intf;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientRandomAccessContainers extends HurriCacheClientInterfaceCommon{
    /**
     * Retrieves an element located at a specific index/position.
     */
    CompletableFuture<Payload> getElementAtPosition(byte[] key,
                                                    KeyHintData hint,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Payload> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, KeyHintData hint, int pos) {
        return getElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }
    /**
     * Atomically fetches and removes an element situated at a specific index/position.
     */
    CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key,
                                                             KeyHintData hint,
                                                             int pos,
                                                             int clientId,
                                                             Duration timeout);

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key, KeyHintData hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key,
                                                                     KeyHintData hint,
                                                                     int pos,
                                                                     int clientId) {
        return getAndRemoveElementAtPosition(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Inserts elements at a target index/position within a container.
     */
    CompletableFuture<Integer> addElementToPosition(byte[] key,
                                                    KeyHintData hint,
                                                    List<Payload> data,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementToPosition(String key, List<Payload> data, int pos) {
        return addElementToPosition(serializeKey(key), null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, KeyHintData hint, List<Payload> data, int pos) {
        return addElementToPosition(serializeKey(key), hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, List<Payload> data, int pos, int clientId) {
        return addElementToPosition(serializeKey(key), null, data, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, KeyHintData hint, List<Payload> data, int pos) {
        return addElementToPosition(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Removes a range of elements by position indexes.
     */
    CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                       KeyHintData hint,
                                                       int pos,
                                                       int endPos,
                                                       int clientId,
                                                       Duration timeout);

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, int pos, int endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, int pos) {
        return removeElementAtPosition(key, hint, pos, pos + 1, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return removeElementAtPosition(key, hint, pos, pos + 1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key,
                                                               KeyHintData hint,
                                                               int pos,
                                                               int endPos,
                                                               int clientId) {
        return removeElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, int pos, int endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }
}