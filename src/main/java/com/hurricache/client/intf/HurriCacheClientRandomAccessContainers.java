package com.hurricache.client.intf;

import com.hurricache.client.intf.Variant;
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
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    CompletableFuture<Variant> getElementAtPosition(byte[] key,
                                                    KeyHintData hint,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Variant> getElementAtPosition(String key, KeyHintData hint, int pos,
                                                            int clientId, Duration timeout) {
        return getElementAtPosition(serializeKey(key), hint, pos, clientId, timeout);
    }

    default CompletableFuture<Variant> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getElementAtPosition(byte[] key, int pos) {
        return getElementAtPosition(key, null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getElementAtPosition(String key, KeyHintData hint, int pos) {
        return getElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getElementAtPosition(byte[] key, int pos, int clientId) {
        return getElementAtPosition(key, null, pos, clientId, getDefaultTimeout());
    }

    /**
     * Atomically fetches and deletes an element situated at a specific index/position.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the deleted variant.
     */
    CompletableFuture<Variant> getAndDeleteElementAtPosition(byte[] key,
                                                             KeyHintData hint,
                                                             int pos,
                                                             int clientId,
                                                             Duration timeout);

    default CompletableFuture<Variant> getAndDeleteElementAtPosition(String key, KeyHintData hint, int pos,
                                                                     int clientId, Duration timeout) {
        return getAndDeleteElementAtPosition(serializeKey(key), hint, pos, clientId, timeout);
    }

    default CompletableFuture<Variant> getAndDeleteElementAtPosition(String key, KeyHintData hint, int pos) {
        return getAndDeleteElementAtPosition(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteElementAtPosition(byte[] key, KeyHintData hint, int pos) {
        return getAndDeleteElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteElementAtPosition(String key,
                                                                     KeyHintData hint,
                                                                     int pos,
                                                                     int clientId) {
        return getAndDeleteElementAtPosition(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAndDeleteElementAtPosition(byte[] key, KeyHintData hint, int pos,
                                                                     int clientId) {
        return getAndDeleteElementAtPosition(key, hint, pos, clientId, getDefaultTimeout());
    }

    /**
     * Inserts elements at a target index/position within a container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of Variant values to insert.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements inserted.
     */
    CompletableFuture<Integer> addElementToPosition(byte[] key,
                                                    KeyHintData hint,
                                                    List<Variant> data,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementToPosition(String key, KeyHintData hint, List<Variant> data,
                                                            int pos, int clientId, Duration timeout) {
        return addElementToPosition(serializeKey(key), hint, data, pos, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToPosition(String key, List<Variant> data, int pos) {
        return addElementToPosition(serializeKey(key), null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, List<Variant> data, int pos) {
        return addElementToPosition(key, null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, KeyHintData hint, List<Variant> data, int pos) {
        return addElementToPosition(serializeKey(key), hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, KeyHintData hint, List<Variant> data, int pos) {
        return addElementToPosition(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(String key, List<Variant> data, int pos, int clientId) {
        return addElementToPosition(serializeKey(key), null, data, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPosition(byte[] key, List<Variant> data, int pos, int clientId) {
        return addElementToPosition(key, null, data, pos, clientId, getDefaultTimeout());
    }

    /**
     * Deletes a range of elements by position indexes.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      start position.
     * @param endPos   end position (-1 for single element).
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements deleted.
     */
    CompletableFuture<Integer> deleteElementAtPosition(byte[] key,
                                                       KeyHintData hint,
                                                       long pos,
                                                       long endPos,
                                                       int clientId,
                                                       Duration timeout);

    default CompletableFuture<Integer> deleteElementAtPosition(String key, KeyHintData hint, long pos, long endPos,
                                                               int clientId, Duration timeout) {
        return deleteElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, timeout);
    }

    default CompletableFuture<Integer> deleteElementAtPosition(String key, KeyHintData hint, long pos, long endPos) {
        return deleteElementAtPosition(serializeKey(key), hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteElementAtPosition(byte[] key, KeyHintData hint, long pos) {
        return deleteElementAtPosition(key, hint, pos, -1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteElementAtPosition(String key, KeyHintData hint, long pos) {
        return deleteElementAtPosition(serializeKey(key), hint, pos, -1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteElementAtPosition(byte[] key, KeyHintData hint, long pos, long endPos) {
        return deleteElementAtPosition(key, hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteElementAtPosition(String key, KeyHintData hint, long pos, long endPos,
                                                               int clientId) {
        return deleteElementAtPosition(serializeKey(key), hint, pos, endPos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> deleteElementAtPosition(byte[] key, KeyHintData hint, long pos, long endPos,
                                                               int clientId) {
        return deleteElementAtPosition(key, hint, pos, endPos, clientId, getDefaultTimeout());
    }

    /**
     * Inserts elements immediately before a specified pivot element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of Variant values to insert.
     * @param pivot    pivot payload element.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements inserted.
     */
    CompletableFuture<Integer> addElementToPositionBefore(byte[] key,
                                                          KeyHintData hint,
                                                          List<Variant> data,
                                                          Variant pivot,
                                                          int clientId,
                                                          Duration timeout);

    default CompletableFuture<Integer> addElementToPositionBefore(String key, KeyHintData hint, List<Variant> data,
                                                                  Variant pivot, int clientId, Duration timeout) {
        return addElementToPositionBefore(serializeKey(key), hint, data, pivot, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToPositionBefore(String key, List<Variant> data, Variant pivot) {
        return addElementToPositionBefore(serializeKey(key), null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(byte[] key, List<Variant> data, Variant pivot) {
        return addElementToPositionBefore(key, null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(String key, KeyHintData hint, List<Variant> data,
                                                                  Variant pivot) {
        return addElementToPositionBefore(serializeKey(key), hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(byte[] key, KeyHintData hint, List<Variant> data,
                                                                  Variant pivot) {
        return addElementToPositionBefore(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(String key, List<Variant> data,
                                                                  Variant pivot, int clientId) {
        return addElementToPositionBefore(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionBefore(byte[] key, List<Variant> data,
                                                                  Variant pivot, int clientId) {
        return addElementToPositionBefore(key, null, data, pivot, clientId, getDefaultTimeout());
    }

    /**
     * Inserts elements immediately after a specified pivot element.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of Variant values to insert.
     * @param pivot    pivot payload element.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements inserted.
     */
    CompletableFuture<Integer> addElementToPositionAfter(byte[] key,
                                                         KeyHintData hint,
                                                         List<Variant> data,
                                                         Variant pivot,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<Integer> addElementToPositionAfter(String key, KeyHintData hint, List<Variant> data,
                                                                 Variant pivot, int clientId, Duration timeout) {
        return addElementToPositionAfter(serializeKey(key), hint, data, pivot, clientId, timeout);
    }

    default CompletableFuture<Integer> addElementToPositionAfter(String key, List<Variant> data, Variant pivot) {
        return addElementToPositionAfter(serializeKey(key), null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(byte[] key, List<Variant> data, Variant pivot) {
        return addElementToPositionAfter(key, null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(String key, KeyHintData hint, List<Variant> data,
                                                                 Variant pivot) {
        return addElementToPositionAfter(serializeKey(key), hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(byte[] key, KeyHintData hint, List<Variant> data,
                                                                 Variant pivot) {
        return addElementToPositionAfter(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(String key, List<Variant> data,
                                                                 Variant pivot, int clientId) {
        return addElementToPositionAfter(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementToPositionAfter(byte[] key, List<Variant> data,
                                                                 Variant pivot, int clientId) {
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
    CompletableFuture<List<Variant>> streamElementInRangeUnordered(byte[] key,
                                                                   KeyHintData hint,
                                                                   ContainerType containerType,
                                                                   int start,
                                                                   int end,
                                                                   int clientId,
                                                                   Duration timeout);

    default CompletableFuture<List<Variant>> streamElementInRangeUnordered(String key, KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start, int end,
                                                                           int clientId, Duration timeout) {
        return streamElementInRangeUnordered(serializeKey(key), hint, containerType, start, end, clientId, timeout);
    }

    default CompletableFuture<List<Variant>> streamElementInRangeUnordered(String key, KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start, int end) {
        return streamElementInRangeUnordered(serializeKey(key), hint, containerType, start, end,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamElementInRangeUnordered(byte[] key, KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start, int end) {
        return streamElementInRangeUnordered(key, hint, containerType, start, end,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamElementInRangeUnordered(String key,
                                                                           ContainerType containerType,
                                                                           int start, int end, int clientId) {
        return streamElementInRangeUnordered(serializeKey(key), null, containerType, start, end, clientId,
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamElementInRangeUnordered(byte[] key,
                                                                           ContainerType containerType,
                                                                           int start, int end) {
        return streamElementInRangeUnordered(key, null, containerType, start, end,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> streamElementInRangeUnordered(byte[] key,
                                                                           ContainerType containerType,
                                                                           int start, int end, int clientId) {
        return streamElementInRangeUnordered(key, null, containerType, start, end, clientId, getDefaultTimeout());
    }
}