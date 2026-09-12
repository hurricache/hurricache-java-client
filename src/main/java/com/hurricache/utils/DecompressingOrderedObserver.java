package com.hurricache.utils;

import com.hurricache.grpc.ValueResponse;

import java.util.concurrent.CompletableFuture;

public class DecompressingOrderedObserver extends CompletableFutureObserver<ValueResponse, byte[]> {

    public DecompressingOrderedObserver(CompletableFuture<byte[]> future) {
        super(future, valueResponse -> CompressionUtils.decompressIfNeeded(valueResponse.getValueOrdered()));
    }

}