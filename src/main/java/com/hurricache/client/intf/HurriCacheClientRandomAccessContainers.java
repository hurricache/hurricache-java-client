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
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    CompletableFuture<Variant> getAt(byte[] key,
                                     KeyHintData hint,
                                     int pos,
                                     int clientId,
                                     Duration timeout);

    default CompletableFuture<Variant> getAt(String key, KeyHintData hint, int pos,
                                             int clientId, Duration timeout) {
        return getAt(serializeKey(key), hint, pos, clientId, timeout);
    }

    default CompletableFuture<Variant> getAt(String key, int pos) {
        return getAt(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAt(byte[] key, int pos) {
        return getAt(key, null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAt(String key, KeyHintData hint, int pos) {
        return getAt(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAt(byte[] key, KeyHintData hint, int pos) {
        return getAt(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAt(String key, int pos, int clientId) {
        return getAt(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> getAt(byte[] key, int pos, int clientId) {
        return getAt(key, null, pos, clientId, getDefaultTimeout());
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
    CompletableFuture<Variant> popAt(byte[] key,
                                     KeyHintData hint,
                                     int pos,
                                     int clientId,
                                     Duration timeout);

    default CompletableFuture<Variant> popAt(String key, KeyHintData hint, int pos,
                                             int clientId, Duration timeout) {
        return popAt(serializeKey(key), hint, pos, clientId, timeout);
    }

    default CompletableFuture<Variant> popAt(String key, KeyHintData hint, int pos) {
        return popAt(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popAt(byte[] key, KeyHintData hint, int pos) {
        return popAt(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Variant> popAt(String key,
                                             KeyHintData hint,
                                             int pos,
                                             int clientId) {
        return popAt(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Variant> popAt(byte[] key, KeyHintData hint, int pos,
                                             int clientId) {
        return popAt(key, hint, pos, clientId, getDefaultTimeout());
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
    CompletableFuture<Integer> insertAt(byte[] key,
                                        KeyHintData hint,
                                        List<Variant> data,
                                        int pos,
                                        int clientId,
                                        Duration timeout);

    default CompletableFuture<Integer> insertAt(String key, KeyHintData hint, List<Variant> data,
                                                int pos, int clientId, Duration timeout) {
        return insertAt(serializeKey(key), hint, data, pos, clientId, timeout);
    }

    default CompletableFuture<Integer> insertAt(String key, List<Variant> data, int pos) {
        return insertAt(serializeKey(key), null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAt(byte[] key, List<Variant> data, int pos) {
        return insertAt(key, null, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAt(String key, KeyHintData hint, List<Variant> data, int pos) {
        return insertAt(serializeKey(key), hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAt(byte[] key, KeyHintData hint, List<Variant> data, int pos) {
        return insertAt(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAt(String key, List<Variant> data, int pos, int clientId) {
        return insertAt(serializeKey(key), null, data, pos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAt(byte[] key, List<Variant> data, int pos, int clientId) {
        return insertAt(key, null, data, pos, clientId, getDefaultTimeout());
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
    CompletableFuture<Integer> removeAt(byte[] key,
                                        KeyHintData hint,
                                        long pos,
                                        long endPos,
                                        int clientId,
                                        Duration timeout);

    default CompletableFuture<Integer> removeAt(String key, KeyHintData hint, long pos, long endPos,
                                                int clientId, Duration timeout) {
        return removeAt(serializeKey(key), hint, pos, endPos, clientId, timeout);
    }

    default CompletableFuture<Integer> removeAt(String key, KeyHintData hint, long pos, long endPos) {
        return removeAt(serializeKey(key), hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeAt(byte[] key, KeyHintData hint, long pos) {
        return removeAt(key, hint, pos, -1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeAt(String key, KeyHintData hint, long pos) {
        return removeAt(serializeKey(key), hint, pos, -1, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeAt(byte[] key, KeyHintData hint, long pos, long endPos) {
        return removeAt(key, hint, pos, endPos, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeAt(String key, KeyHintData hint, long pos, long endPos,
                                                int clientId) {
        return removeAt(serializeKey(key), hint, pos, endPos, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeAt(byte[] key, KeyHintData hint, long pos, long endPos,
                                                int clientId) {
        return removeAt(key, hint, pos, endPos, clientId, getDefaultTimeout());
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
    CompletableFuture<Integer> insertBefore(byte[] key,
                                            KeyHintData hint,
                                            List<Variant> data,
                                            Variant pivot,
                                            int clientId,
                                            Duration timeout);

    default CompletableFuture<Integer> insertBefore(String key, KeyHintData hint, List<Variant> data,
                                                    Variant pivot, int clientId, Duration timeout) {
        return insertBefore(serializeKey(key), hint, data, pivot, clientId, timeout);
    }

    default CompletableFuture<Integer> insertBefore(String key, List<Variant> data, Variant pivot) {
        return insertBefore(serializeKey(key), null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertBefore(byte[] key, List<Variant> data, Variant pivot) {
        return insertBefore(key, null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertBefore(String key, KeyHintData hint, List<Variant> data,
                                                    Variant pivot) {
        return insertBefore(serializeKey(key), hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertBefore(byte[] key, KeyHintData hint, List<Variant> data,
                                                    Variant pivot) {
        return insertBefore(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertBefore(String key, List<Variant> data,
                                                    Variant pivot, int clientId) {
        return insertBefore(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertBefore(byte[] key, List<Variant> data,
                                                    Variant pivot, int clientId) {
        return insertBefore(key, null, data, pivot, clientId, getDefaultTimeout());
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
    CompletableFuture<Integer> insertAfter(byte[] key,
                                           KeyHintData hint,
                                           List<Variant> data,
                                           Variant pivot,
                                           int clientId,
                                           Duration timeout);

    default CompletableFuture<Integer> insertAfter(String key, KeyHintData hint, List<Variant> data,
                                                   Variant pivot, int clientId, Duration timeout) {
        return insertAfter(serializeKey(key), hint, data, pivot, clientId, timeout);
    }

    default CompletableFuture<Integer> insertAfter(String key, List<Variant> data, Variant pivot) {
        return insertAfter(serializeKey(key), null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAfter(byte[] key, List<Variant> data, Variant pivot) {
        return insertAfter(key, null, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAfter(String key, KeyHintData hint, List<Variant> data,
                                                   Variant pivot) {
        return insertAfter(serializeKey(key), hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAfter(byte[] key, KeyHintData hint, List<Variant> data,
                                                   Variant pivot) {
        return insertAfter(key, hint, data, pivot, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAfter(String key, List<Variant> data,
                                                   Variant pivot, int clientId) {
        return insertAfter(serializeKey(key), null, data, pivot, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Integer> insertAfter(byte[] key, List<Variant> data,
                                                   Variant pivot, int clientId) {
        return insertAfter(key, null, data, pivot, clientId, getDefaultTimeout());
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
    CompletableFuture<List<Variant>> getRange(byte[] key,
                                              KeyHintData hint,
                                              ContainerType containerType,
                                              int start,
                                              int end,
                                              int clientId,
                                              Duration timeout);

    default CompletableFuture<List<Variant>> getRange(String key, KeyHintData hint,
                                                      ContainerType containerType,
                                                      int start, int end,
                                                      int clientId, Duration timeout) {
        return getRange(serializeKey(key), hint, containerType, start, end, clientId, timeout);
    }

    default CompletableFuture<List<Variant>> getRange(String key, KeyHintData hint,
                                                      ContainerType containerType,
                                                      int start, int end) {
        return getRange(serializeKey(key), hint, containerType, start, end,
                        getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getRange(byte[] key, KeyHintData hint,
                                                      ContainerType containerType,
                                                      int start, int end) {
        return getRange(key, hint, containerType, start, end,
                        getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getRange(String key,
                                                      ContainerType containerType,
                                                      int start, int end, int clientId) {
        return getRange(serializeKey(key), null, containerType, start, end, clientId,
                        getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getRange(byte[] key,
                                                      ContainerType containerType,
                                                      int start, int end) {
        return getRange(key, null, containerType, start, end,
                        getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Variant>> getRange(byte[] key,
                                                      ContainerType containerType,
                                                      int start, int end, int clientId) {
        return getRange(key, null, containerType, start, end, clientId, getDefaultTimeout());
    }
}