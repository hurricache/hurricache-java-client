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

    /**
     * Inserts elements immediately before a specified pivot element.
     */
    CompletableFuture<Boolean> addElementToPositionBefore(byte[] key,
                                                          KeyHintData hint,
                                                          List<Payload> data,
                                                          Payload pivot,
                                                          int clientId,
                                                          Duration timeout);

    default CompletableFuture<Boolean> addElementToPositionBefore(String key, List<Payload> data, Payload pivot) {
        return addElementToPositionBefore(serializeKey(key),
                                          null,
                                          data,
                                          pivot,
                                          getDefaultClientId(),
                                          getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionBefore(String key,
                                                                  KeyHintData hint,
                                                                  List<Payload> data,
                                                                  Payload pivot) {
        return addElementToPositionBefore(serializeKey(key),
                                          hint,
                                          data,
                                          pivot,
                                          getDefaultClientId(),
                                          getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionBefore(String key,
                                                                  List<Payload> data,
                                                                  Payload pivot,
                                                                  int clientId) {
        return addElementToPositionBefore(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionBefore(byte[] key,
                                                                  KeyHintData hint,
                                                                  List<Payload> data,
                                                                  Payload pivot) {
        return addElementToPositionBefore(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Inserts elements immediately after a specified pivot element.
     */
    CompletableFuture<Boolean> addElementToPositionAfter(byte[] key,
                                                         KeyHintData hint,
                                                         List<Payload> data,
                                                         Payload pivot,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<Boolean> addElementToPositionAfter(String key, List<Payload> data, Payload pivot) {
        return addElementToPositionAfter(serializeKey(key),
                                         null,
                                         data,
                                         pivot,
                                         getDefaultClientId(),
                                         getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionAfter(String key,
                                                                 KeyHintData hint,
                                                                 List<Payload> data,
                                                                 Payload pivot) {
        return addElementToPositionAfter(serializeKey(key),
                                         hint,
                                         data,
                                         pivot,
                                         getDefaultClientId(),
                                         getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionAfter(String key,
                                                                 List<Payload> data,
                                                                 Payload pivot,
                                                                 int clientId) {
        return addElementToPositionAfter(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToPositionAfter(byte[] key,
                                                                 KeyHintData hint,
                                                                 List<Payload> data,
                                                                 Payload pivot) {
        return addElementToPositionAfter(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }
}