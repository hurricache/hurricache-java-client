package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientRandomAccessContainers extends HurriCacheClientInterfaceCommon{
    /**
     * Retrieves an element located at a specific index/position.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the payload at the position.
     */
    CompletableFuture<Payload> getElementAtPosition(byte[] key,
                                                    KeyHintData hint,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Payload> getElementAtPosition(String key, KeyHintData hint, int pos,
                                                            int clientId, Duration timeout) {
        return getElementAtPosition(serializeKey(key), hint, pos, clientId, timeout);
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(byte[] key, int pos) {
        return getElementAtPosition(key, null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, KeyHintData hint, int pos) {
        return getElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getElementAtPosition(byte[] key, int pos, int clientId) {
        return getElementAtPosition(key, null, pos, clientId, getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element situated at a specific index/position.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed payload.
     */
    CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key,
                                                             KeyHintData hint,
                                                             int pos,
                                                             int clientId,
                                                             Duration timeout);

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key, KeyHintData hint, int pos,
                                                                     int clientId, Duration timeout) {
        return getAndRemoveElementAtPosition(serializeKey(key), hint, pos, clientId, timeout);
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key, KeyHintData hint, int pos) {
        return getAndRemoveElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(String key,
                                                                     KeyHintData hint,
                                                                     int pos,
                                                                     int clientId) {
        return getAndRemoveElementAtPosition(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key, KeyHintData hint, int pos,
                                                                     int clientId) {
        return getAndRemoveElementAtPosition(key, hint, pos, clientId, getDefaultTimeout());
    }

    /**
     * Inserts elements at a target index/position within a container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of payloads to insert.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements inserted.
     */
    CompletableFuture<Integer> addElementToPosition(byte[] key,
                                                    KeyHintData hint,
                                                    List<Payload> data,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementToPosition(String key, KeyHintData hint, List<Payload> data,
                                                            int pos, int clientId, Duration timeout) {
        return addElementToPosition(serializeKey(key), hint, data, pos, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToPosition(String key, List<Payload> data, int pos) {
        return addElementToPosition(serializeKey(key), null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, List<Payload> data, int pos) {
        return addElementToPosition(key, null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, KeyHintData hint, List<Payload> data, int pos) {
        return addElementToPosition(serializeKey(key), hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, KeyHintData hint, List<Payload> data, int pos) {
        return addElementToPosition(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, List<Payload> data, int pos, int clientId) {
        return addElementToPosition(serializeKey(key), null, data, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, List<Payload> data, int pos, int clientId) {
        return addElementToPosition(key, null, data, pos, clientId, getDefaultTimeout());
    }

    /**
     * Removes a range of elements by position indexes.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      start position.
     * @param endPos   end position (-1 for single element).
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing true if successful.
     */
    CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                       KeyHintData hint,
                                                       long pos,
                                                       long endPos,
                                                       int clientId,
                                                       Duration timeout);

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, long pos, long endPos,
                                                               int clientId, Duration timeout) {
        return removeElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, timeout);
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, long pos, long endPos) {
        return removeElementAtPosition(serializeKey(key), hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, long pos) {
        return removeElementAtPosition(key, hint, pos, -1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, long pos) {
        return removeElementAtPosition(serializeKey(key), hint, pos, -1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, long pos, long endPos) {
        return removeElementAtPosition(key, hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, KeyHintData hint, long pos, long endPos,
                                                               int clientId) {
        return removeElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, long pos, long endPos,
                                                               int clientId) {
        return removeElementAtPosition(key, hint, pos, endPos, clientId, getDefaultTimeout());
    }

    /**
     * Inserts elements immediately before a specified pivot element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of payloads to insert.
     * @param pivot    pivot payload element.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements inserted.
     */
    CompletableFuture<Integer> addElementToPositionBefore(byte[] key,
                                                          KeyHintData hint,
                                                          List<Payload> data,
                                                          Payload pivot,
                                                          int clientId,
                                                          Duration timeout);

    default CompletableFuture<Integer> addElementToPositionBefore(String key, KeyHintData hint, List<Payload> data,
                                                                  Payload pivot, int clientId, Duration timeout) {
        return addElementToPositionBefore(serializeKey(key), hint, data, pivot, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToPositionBefore(String key, List<Payload> data, Payload pivot) {
        return addElementToPositionBefore(serializeKey(key), null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(byte[] key, List<Payload> data, Payload pivot) {
        return addElementToPositionBefore(key, null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(String key, KeyHintData hint, List<Payload> data,
                                                                  Payload pivot) {
        return addElementToPositionBefore(serializeKey(key), hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(byte[] key, KeyHintData hint, List<Payload> data,
                                                                  Payload pivot) {
        return addElementToPositionBefore(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(String key, List<Payload> data,
                                                                  Payload pivot, int clientId) {
        return addElementToPositionBefore(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(byte[] key, List<Payload> data,
                                                                  Payload pivot, int clientId) {
        return addElementToPositionBefore(key, null, data, pivot, clientId, getDefaultTimeout());
    }

    /**
     * Inserts elements immediately after a specified pivot element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of payloads to insert.
     * @param pivot    pivot payload element.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements inserted.
     */
    CompletableFuture<Integer> addElementToPositionAfter(byte[] key,
                                                         KeyHintData hint,
                                                         List<Payload> data,
                                                         Payload pivot,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<Integer> addElementToPositionAfter(String key, KeyHintData hint, List<Payload> data,
                                                                 Payload pivot, int clientId, Duration timeout) {
        return addElementToPositionAfter(serializeKey(key), hint, data, pivot, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToPositionAfter(String key, List<Payload> data, Payload pivot) {
        return addElementToPositionAfter(serializeKey(key), null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(byte[] key, List<Payload> data, Payload pivot) {
        return addElementToPositionAfter(key, null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(String key, KeyHintData hint, List<Payload> data,
                                                                 Payload pivot) {
        return addElementToPositionAfter(serializeKey(key), hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(byte[] key, KeyHintData hint, List<Payload> data,
                                                                 Payload pivot) {
        return addElementToPositionAfter(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(String key, List<Payload> data,
                                                                 Payload pivot, int clientId) {
        return addElementToPositionAfter(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(byte[] key, List<Payload> data,
                                                                 Payload pivot, int clientId) {
        return addElementToPositionAfter(key, null, data, pivot, clientId, getDefaultTimeout());
    }

    /**
     * Fetches a slice of elements from an unordered container based on position indexes.
     *
     * @param key           target container key in byte array form.
     * @param hint          optional key routing hint.
     * @param containerType type of container.
     * @param start         start index.
     * @param end           end index.
     * @param clientId      identifier of the issuing client.
     * @param timeout       execution timeout duration.
     * @return a {@link CompletableFuture} containing list of payloads in range.
     */
    CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                   KeyHintData hint,
                                                                   ContainerType containerType,
                                                                   int start,
                                                                   int end,
                                                                   int clientId,
                                                                   Duration timeout);

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key, KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start, int end,
                                                                           int clientId, Duration timeout) {
        return streamElementInRangeUnordered(serializeKey(key), hint, containerType, start, end, clientId, timeout);
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key, KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start, int end) {
        return streamElementInRangeUnordered(serializeKey(key), hint, containerType, start, end,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key, KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start, int end) {
        return streamElementInRangeUnordered(key, hint, containerType, start, end,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           ContainerType containerType,
                                                                           int start, int end, int clientId) {
        return streamElementInRangeUnordered(serializeKey(key), null, containerType, start, end, clientId,
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                           ContainerType containerType,
                                                                           int start, int end) {
        return streamElementInRangeUnordered(key, null, containerType, start, end,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                           ContainerType containerType,
                                                                           int start, int end, int clientId) {
        return streamElementInRangeUnordered(key, null, containerType, start, end, clientId, getDefaultTimeout());
    }
}