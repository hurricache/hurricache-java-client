package com.hurricache.utils;

import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BatchValueResponse;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Observer that converts BatchValueResponse to List<Variant>.
 */
public class StreamBatchVariantObserver implements StreamObserver<BatchValueResponse> {

    private final CompletableFuture<List<Variant>> future;
    private final List<Variant> result = new ArrayList<>();

    public StreamBatchVariantObserver(CompletableFuture<List<Variant>> future) {
        this.future = future;
    }

    @Override
    public void onNext(BatchValueResponse response) {
        for (int i = 0; i < response.getValueUnorderedCount(); i++) {
            com.hurricache.grpc.Value val = response.getValueUnordered(i);
            Variant variant = extractVariant(val);
            result.add(variant);
        }
    }

    @Override
    public void onError(Throwable t) {
        future.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
        future.complete(result);
    }

    private Variant extractVariant(com.hurricache.grpc.Value val) {
        com.hurricache.grpc.BinaryPayload bp = val.getValue();
        com.hurricache.grpc.ValueType vt = val.hasValueType() ? val.getValueType() : com.hurricache.grpc.ValueType.AUTO;

        switch (vt) {
            case INT:
                return Variant.ofInt(bp.getIntPayload());
            case DOUBLE:
                return Variant.ofDouble(bp.getDoublePayload());
            case BYTES:
            case AUTO:
            default:
                if (bp.hasIntPayload()) return Variant.ofInt(bp.getIntPayload());
                if (bp.hasDoublePayload()) return Variant.ofDouble(bp.getDoublePayload());
                if (bp.hasPayload()) return Variant.ofBytes(bp.getPayload().toByteArray());
                return Variant.none();
        }
    }
}
