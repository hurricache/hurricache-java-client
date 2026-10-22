package com.hurricache.utils;

import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BatchValueResponse;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Observer that converts BatchValueResponse with ordered values to Map<OrderedPayload, Variant>.
 */
public class StreamBatchOrderedMapVariantObserver implements StreamObserver<BatchValueResponse> {

    private final CompletableFuture<Map<OrderedPayload, Variant>> future;
    private final Map<OrderedPayload, Variant> result = new HashMap<>();

    public StreamBatchOrderedMapVariantObserver(CompletableFuture<Map<OrderedPayload, Variant>> future) {
        this.future = future;
    }

    @Override
    public void onNext(BatchValueResponse response) {
        int count = response.getKeyOrderedCount();
        
        for (int i = 0; i < count; i++) {
            byte[] keyBytes = response.getKeyOrdered(i).getPayload().getPayload().toByteArray();;
            long weight = response.getKeyOrdered(i).getOrder();
            // Server sends values in value_unordered field
            Variant variant = extractVariantFromValue(response.getValueUnordered(i));
            result.put(OrderedPayload.of(keyBytes, weight), variant);
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

    private Variant extractVariant(com.hurricache.grpc.OrderedValue val) {
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

    private Variant extractVariantFromValue(com.hurricache.grpc.Value val) {
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