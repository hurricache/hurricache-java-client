package com.hurricache.utils;

import com.hurricache.grpc.UpdateValueResponse;
import com.hurricache.grpc.ValueResponse;

import java.util.concurrent.CompletableFuture;

public class DecompressingObserver extends CompletableFutureObserver<ValueResponse, byte[]> {

    public DecompressingObserver(CompletableFuture<byte[]> future) {
        super(future, CompressionUtils::decompressIfNeeded);
    }

    public static class Update extends CompletableFutureObserver<UpdateValueResponse, byte[]> {

        public Update(CompletableFuture<byte[]> future) {
            super(future, CompressionUtils::decompressIfNeeded);
        }
    }
}