package com.hurricache.client.intf;

import com.hurricache.grpc.KeyHint;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HurriCacheClientList extends HurriCacheClientInterfaceCommon,HurriCacheClientDequeBased,HurriCacheClientRandomAccessContainers{
    /**
    * Creates a List container (linked sequence).
    */
    CompletableFuture<KeyHintData> createList(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout);

    default CompletableFuture<KeyHintData> createList(byte[] key, List<Payload> initialValue, Duration ttl) {
        return createList(key,null , initialValue, ttl, getDefaultClientId(), getDefaultTimeout() );
    }

    default CompletableFuture<KeyHintData> createList(String key) {
        return createList(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHintData> createList(String key, List<Payload> initialValue) {
        return createList(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHintData> createList(byte[] key, List<Payload> initialValue) {
        return createList(key,null , initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout() );
    }
    default CompletableFuture<KeyHintData> createList(byte[] key,KeyHintData keyHint, List<Payload> initialValue) {
        return createList(key,keyHint , initialValue, getDefaultTtl(), getDefaultClientId(), getDefaultTimeout() );
    }

    default CompletableFuture<KeyHintData> createList(String key, List<Payload> initialValue, int clientId) {
        return createList(serializeKey(key), null, initialValue == null
                                                   ? Collections.emptyList()
                                                   : initialValue,
                          getDefaultTtl(),
                          clientId,
                          getDefaultTimeout() );
    }
    /**
     * Streams or retrieves all elements contained in a List container.
     */
    CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint, int clientId, Duration timeout);

    default CompletableFuture<List<Payload>> streamList(String key) {
        return streamList(key, getDefaultClientId());
    }

    default CompletableFuture<List<Payload>> streamList(String key, int clientId) {
        return streamList(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamList(String key, KeyHintData hint) {
        return streamList(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint) {
        return streamList(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

}