package com.hurricache.client.intf;

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

    /**
     * Gracefully shuts down client connections and releases network resources.
     */
    void shutdown();
    long MAX_RPC_SIZE = 4 * 1024 * 1024 - 1024 * 1024 / 2;
    enum AddType { TAIL, HEAD, POSITION,NON_POSITION }
}