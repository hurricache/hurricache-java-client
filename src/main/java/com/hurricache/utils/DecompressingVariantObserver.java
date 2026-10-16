package com.hurricache.utils;

import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ValueResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Observer that converts ValueResponse to Variant.
 * Handles INT/DOUBLE/BYTES correctly with decompression support.
 */
public class DecompressingVariantObserver extends CompletableFutureObserver<ValueResponse, Variant> {

    public DecompressingVariantObserver(CompletableFuture<Variant> future) {
        super(future, valueResponse -> {
            if (valueResponse.hasValueUnordered()) {
                return CompressionUtils.decompressIfNeededToVariant(valueResponse.getValueUnordered());
            }
            if (valueResponse.hasValueOrdered()) {
                return CompressionUtils.decompressIfNeededToVariant(valueResponse.getValueOrdered());
            }
            return Variant.none();
        });
    }
}