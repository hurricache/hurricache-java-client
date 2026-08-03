package com.hurricache.client.intf;

import com.hurricache.grpc.ContainerType;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous client interface for interacting with the HurriCache distributed caching system.
 * <p>
 * Provides high-performance async methods for managing atomic counters/values, standard Key-Value pairs,
 * distributed locks, as well as ordered ({@link OrderedPayload}) and unordered ({@link Payload})
 * data structures (Queue, List, Vector, Set, Map).
 */
public interface HurriCacheClientInterface extends HurriCacheClientRaw,
                                                   HurriCacheClientInterfaceCommon,
                                                   HurriCacheClientQueue,
                                                   HurriCacheClientHashMap,
                                                   HurriCacheClientList,
                                                   HurriCacheClientVector,
                                                   HurriCacheClientSet,
                                                   HurriCacheClientSortedMap,
                                                   HurriCacheClientSortedSet,
                                                   HurriCacheClientAtomics {

    // =========================================================================
    // TTL & BASIC KEY-VALUE OPERATIONS
    // =========================================================================











    /**
     * Adds elements to an unordered container (e.g., Set,HashSet).
     */
    CompletableFuture<Boolean> addElement(byte[] key,
                                          KeyHintData hint,
                                          List<Payload> data,
                                          int clientId,
                                          Duration timeout);

    default CompletableFuture<Boolean> addElement(String key, List<Payload> data) {
        return addElement(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElement(String key, KeyHintData hint, List<Payload> data) {
        return addElement(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElement(byte[] key, KeyHintData hint, List<Payload> data) {
        return addElement(key, hint, data, getDefaultClientId(), getDefaultTimeout());
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

    /**
     * Fetches a slice (range) of elements from an unordered container based on position indexes.
     */
    CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                   KeyHintData hint,
                                                                   ContainerType containerType,
                                                                   int start,
                                                                   int end,
                                                                   int clientId,
                                                                   Duration timeout);

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end) {
        return streamElementInRangeUnordered(serializeKey(key),
                                             hint,
                                             containerType,
                                             start,
                                             end,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(String key,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end,
                                                                           int clientId) {
        return streamElementInRangeUnordered(serializeKey(key),
                                             null,
                                             containerType,
                                             start,
                                             end,
                                             clientId,
                                             getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                           KeyHintData hint,
                                                                           ContainerType containerType,
                                                                           int start,
                                                                           int end) {
        return streamElementInRangeUnordered(key,
                                             hint,
                                             containerType,
                                             start,
                                             end,
                                             getDefaultClientId(),
                                             getDefaultTimeout());
    }

    /**
     * Deletes matching values/keys from the given container type.
     *
     * @return count of successfully removed elements.
     */
    CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                   KeyHintData hint,
                                                   ContainerType type,
                                                   List<Payload> values,
                                                   List<Payload> keys,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<Integer> removeFromContainer(String key, ContainerType type, List<Payload> values) {
        return removeFromContainer(serializeKey(key),
                                   null,
                                   type,
                                   values,
                                   Collections.emptyList(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    default CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                           KeyHintData hint,
                                                           ContainerType type,
                                                           List<Payload> values) {
        return removeFromContainer(key,
                                   hint,
                                   type,
                                   values,
                                   Collections.emptyList(),
                                   getDefaultClientId(),
                                   getDefaultTimeout());
    }

    // =========================================================================
    // ORDERED CONTAINERS (OrderedPayload)
    // =========================================================================

    /**
     * Fetches a sub-range of elements from an {@link OrderedSet} filtered by score/weight boundaries.
     *
     * @param startWeight lower bound weight limit.
     * @param endWeight   upper bound weight limit.
     * @param reverse     {@code true} for descending ordering, {@code false} for ascending.
     */
    CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key,
                                                                           KeyHintData hint,
                                                                           long startWeight,
                                                                           long endWeight,
                                                                           boolean reverse,
                                                                           int clientId,
                                                                           Duration timeout);

    default CompletableFuture<List<OrderedPayload>> streamElementInRangeOrdered(String key,
                                                                                ContainerType containerType,
                                                                                long startWeight,
                                                                                long endWeight) {
        return streamElementInRangeOrderedSet(serializeKey(key),
                                              null,
                                              startWeight,
                                              endWeight,
                                              false,
                                              getDefaultClientId(),
                                              getDefaultTimeout());
    }

    /**
     * Adds weighted elements to an {@link OrderedSet}.
     */
    CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                    KeyHintData hint,
                                                    List<OrderedPayload> data,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Integer> addElementWithWeight(String key, List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), null, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(String key, KeyHintData hint, List<OrderedPayload> data) {
        return addElementWithWeight(serializeKey(key), hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint, List<OrderedPayload> data) {
        return addElementWithWeight(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    // =========================================================================
    // ATOMIC & LOCK OPERATIONS
    // =========================================================================


    /**
     * Gracefully shuts down client connections and releases network resources.
     */
    void shutdown();
}