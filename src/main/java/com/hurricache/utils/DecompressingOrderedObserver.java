package com.hurricache.utils;

import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ValueResponse;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DecompressingOrderedObserver extends CompletableFutureObserver<ValueResponse, Variant> {

    public DecompressingOrderedObserver(CompletableFuture<Variant> future) {
        super(future, valueResponse -> {
            if (!valueResponse.hasValueOrdered()) {
                return Variant.none();
            }
            return CompressionUtils.decompressIfNeededToVariant(valueResponse.getValueOrdered());
        });
    }

    public DecompressingOrderedObserver(CompletableFuture<Variant> future, Function<ValueResponse, Variant> function) {
        super(future, function);
    }
}