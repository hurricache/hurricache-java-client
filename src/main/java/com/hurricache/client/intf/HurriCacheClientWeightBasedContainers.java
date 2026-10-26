package com.hurricache.client.intf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientWeightBasedContainers extends HurriCacheClientInterfaceCommon{
    /**
     * Retrieves an element located at a specific index/position in a weight-based ordered container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    CompletableFuture<Variant> getElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    /**
     * Retrieves an element at the given position using a string key.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(String key, KeyHintData hint, int pos,
                                                            int clientId, Duration timeout) {
        return getElementWithWeight(serializeKey(key), hint, pos, clientId, timeout);
    }

    /**
     * Retrieves an element at the given position using a string key and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(String key, KeyHintData hint, int pos,
                                                            int clientId) {
        return getElementWithWeight(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using a string key and default client settings.
     *
     * @param key target container key as a string (encoded as UTF-8).
     * @param pos position index.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(String key, int pos) {
        return getElementWithWeight(serializeKey(key), null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using a string key and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param hint optional key routing hint.
     * @param pos  position index.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(String key, KeyHintData hint, int pos) {
        return getElementWithWeight(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using a string key and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(String key, int pos, int clientId) {
        return getElementWithWeight(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using default client settings.
     *
     * @param key  target container key in byte array form.
     * @param hint optional key routing hint.
     * @param pos  position index.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(byte[] key, KeyHintData hint, int pos) {
        return getElementWithWeight(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(byte[] key, KeyHintData hint, int pos,
                                                            int clientId) {
        return getElementWithWeight(key, hint, pos, clientId, getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using default client settings.
     *
     * @param key target container key in byte array form.
     * @param pos position index.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(byte[] key, int pos) {
        return getElementWithWeight(key, null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Retrieves an element at the given position using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the variant at the position.
     */
    default CompletableFuture<Variant> getElementWithWeight(byte[] key, int pos, int clientId) {
        return getElementWithWeight(key, null, pos, clientId, getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element situated at a specific index/position
     * in a weight-based ordered container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key,
                                                             KeyHintData hint,
                                                             int pos,
                                                             int clientId,
                                                             Duration timeout);

    /**
     * Atomically fetches and removes an element at the given position using a string key.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(String key, KeyHintData hint, int pos,
                                                                     int clientId, Duration timeout) {
        return getAndRemoveElementWithWeight(serializeKey(key), hint, pos, clientId, timeout);
    }

    /**
     * Atomically fetches and removes an element at the given position using a string key
     * and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param hint optional key routing hint.
     * @param pos  position index.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(String key, KeyHintData hint, int pos) {
        return getAndRemoveElementWithWeight(serializeKey(key), hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using a string key
     * and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(String key,
                                                                     KeyHintData hint,
                                                                     int pos,
                                                                     int clientId) {
        return getAndRemoveElementWithWeight(serializeKey(key), hint, pos, clientId, getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using a string key
     * and default client settings.
     *
     * @param key target container key as a string (encoded as UTF-8).
     * @param pos position index.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(String key, int pos) {
        return getAndRemoveElementWithWeight(serializeKey(key), null, pos,
                                             getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using a string key
     * and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(String key, int pos, int clientId) {
        return getAndRemoveElementWithWeight(serializeKey(key), null, pos, clientId, getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using default client settings.
     *
     * @param key  target container key in byte array form.
     * @param hint optional key routing hint.
     * @param pos  position index.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key, KeyHintData hint, int pos) {
        return getAndRemoveElementWithWeight(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key, KeyHintData hint, int pos,
                                                                     int clientId) {
        return getAndRemoveElementWithWeight(key, hint, pos, clientId, getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using default client settings.
     *
     * @param key target container key in byte array form.
     * @param pos position index.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key, int pos) {
        return getAndRemoveElementWithWeight(key, null, pos, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Atomically fetches and removes an element at the given position using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param pos      position index.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing the removed variant.
     */
    default CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key, int pos, int clientId) {
        return getAndRemoveElementWithWeight(key, null, pos, clientId, getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of {@link OrderedVariant} elements with weights.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    List<OrderedVariant> data,
                                                    int clientId,
                                                    Duration timeout);

    /**
     * Adds elements with weight to an ordered container using a string key.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param data     list of {@link OrderedVariant} elements with weights.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedVariant> data,
                                                            int clientId, Duration timeout) {
        return addElementWithWeight(serializeKey(key), hint, data,  clientId, timeout);
    }

    /**
     * Adds elements with weight to an ordered container using a string key and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param hint optional key routing hint.
     * @param data list of {@link OrderedVariant} elements with weights.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedVariant> data) {
        return addElementWithWeight(serializeKey(key), hint, data,  getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container using a string key and default client settings.
     *
     * @param key  target container key as a string (encoded as UTF-8).
     * @param data list of {@link OrderedVariant} elements with weights.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, List<OrderedVariant> data) {
        return addElementWithWeight(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container using default client settings.
     *
     * @param key  target container key in byte array form.
     * @param hint optional key routing hint.
     * @param data list of {@link OrderedVariant} elements with weights.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint,
                                                            List<OrderedVariant> data) {
        return addElementWithWeight(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container using default client settings.
     *
     * @param key  target container key in byte array form.
     * @param data list of {@link OrderedVariant} elements with weights.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key, List<OrderedVariant> data) {
        return addElementWithWeight(key, null, data, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container using a string key and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param data     list of {@link OrderedVariant} elements with weights.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedVariant> data, int clientId) {
        return addElementWithWeight(serializeKey(key), hint, data,  clientId, getDefaultTimeout());
    }

    /**
     * Adds elements with weight to an ordered container using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param data     list of {@link OrderedVariant} elements with weights.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint,
                                                            List<OrderedVariant> data, int clientId) {
        return addElementWithWeight(key, hint, data, clientId, getDefaultTimeout());
    }

    /**
     * Adds key/value pairs with weights to an ordered map container (ORDERED_MAP).
     * <p>
     * The default implementation discards the explicit {@code values} and adds only the
     * weighted keys (ORDERED_SET behavior) by delegating to
     * {@link #addElementWithWeight(byte[], KeyHintData, List, int, Duration)}.
     * Clients that natively support ORDERED_MAP should override this method.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param keys     list of {@link OrderedPayload} key elements with weights.
     * @param values   list of {@link Variant} values associated with the keys.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                            KeyHintData hint,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values,
                                                            int clientId,
                                                            Duration timeout) {
        List<OrderedVariant> orderedKeys = new ArrayList<>(keys == null ? 0 : keys.size());
        if (keys != null) {
            for (OrderedPayload payloadKey : keys) {
                orderedKeys.add(new OrderedVariant(Variant.ofBytes(payloadKey.getValue()),
                                                   payloadKey.getOrder()));
            }
        }
        return addElementWithWeight(key, hint, orderedKeys, clientId, timeout);
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using a string key.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param keys     list of {@link OrderedPayload} key elements with weights.
     * @param values   list of {@link Variant} values associated with the keys.
     * @param clientId identifier of the issuing client.
     * @param timeout  execution timeout duration.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values,
                                                            int clientId, Duration timeout) {
        return addElementWithWeight(serializeKey(key), hint, keys, values, clientId, timeout);
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using default client settings.
     *
     * @param key    target container key in byte array form.
     * @param hint   optional key routing hint.
     * @param keys   list of {@link OrderedPayload} key elements with weights.
     * @param values list of {@link Variant} values associated with the keys.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values) {
        return addElementWithWeight(key, hint, keys, values, getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using a string key
     * and default client settings.
     *
     * @param key    target container key as a string (encoded as UTF-8).
     * @param hint   optional key routing hint.
     * @param keys   list of {@link OrderedPayload} key elements with weights.
     * @param values list of {@link Variant} values associated with the keys.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values) {
        return addElementWithWeight(serializeKey(key), hint, keys, values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using a string key
     * and default client settings.
     *
     * @param key    target container key as a string (encoded as UTF-8).
     * @param keys   list of {@link OrderedPayload} key elements with weights.
     * @param values list of {@link Variant} values associated with the keys.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values) {
        return addElementWithWeight(serializeKey(key), null, keys, values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using default client settings.
     *
     * @param key    target container key in byte array form.
     * @param keys   list of {@link OrderedPayload} key elements with weights.
     * @param values list of {@link Variant} values associated with the keys.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values) {
        return addElementWithWeight(key, null, keys, values,
                                    getDefaultClientId(), getDefaultTimeout());
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using a string key
     * and the default timeout.
     *
     * @param key      target container key as a string (encoded as UTF-8).
     * @param hint     optional key routing hint.
     * @param keys     list of {@link OrderedPayload} key elements with weights.
     * @param values   list of {@link Variant} values associated with the keys.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values, int clientId) {
        return addElementWithWeight(serializeKey(key), hint, keys, values,
                                    clientId, getDefaultTimeout());
    }

    /**
     * Adds key/value pairs with weights to an ordered map container using the default timeout.
     *
     * @param key      target container key in byte array form.
     * @param hint     optional key routing hint.
     * @param keys     list of {@link OrderedPayload} key elements with weights.
     * @param values   list of {@link Variant} values associated with the keys.
     * @param clientId identifier of the issuing client.
     * @return a {@link CompletableFuture} containing count of elements added.
     */
    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint,
                                                            List<OrderedPayload> keys,
                                                            List<Variant> values, int clientId) {
        return addElementWithWeight(key, hint, keys, values, clientId, getDefaultTimeout());
    }
}
