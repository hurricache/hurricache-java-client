package com.hurricache.utils;

import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BatchValueResponse;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Observer that converts BatchValueResponse to Map<Payload, Variant>.
 */
public class StreamBatchMapVariantObserver implements StreamObserver<BatchValueResponse> {

    private final CompletableFuture<Map<Payload, Variant>> future;
    private final Map<Payload, Variant> result = new HashMap<>();

    public StreamBatchMapVariantObserver(CompletableFuture<Map<Payload, Variant>> future) {
        this.future = future;
    }

    @Override
    public void onNext(BatchValueResponse response) {
        for (int i = 0; i < response.getKeyUnorderedCount(); i++) {
            byte[] keyBytes = response.getKeyUnordered(i).getPayload().toByteArray();
            Variant variant = extractVariant(response.getValueUnordered(i));
            result.put(Payload.of(keyBytes), variant);
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
                return Variant.ofLong(bp.getIntPayload());
            case DOUBLE:
                return Variant.ofDouble(bp.getDoublePayload());
            case BYTES:
            case AUTO:
            default:
                if (bp.hasIntPayload()) return Variant.ofLong(bp.getIntPayload());
                if (bp.hasDoublePayload()) return Variant.ofDouble(bp.getDoublePayload());
                if (bp.hasPayload()) return Variant.ofBytes(bp.getPayload().toByteArray());
                return Variant.none();
        }
    }
}