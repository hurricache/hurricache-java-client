package com.hurricache.utils;

import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamBatchOrderedObserver extends CompletableFutureObserver<BatchValueResponse, List<OrderedVariant>> {

    public StreamBatchOrderedObserver(CompletableFuture<List<OrderedVariant>> future) {
        super(future, res -> res.getValueOrderedList().stream()
                .map(orderedValue ->{
                        return switch (orderedValue.getValueType()){
                            case AUTO -> OrderedVariant.of(Variant.none());
                            case BYTES -> OrderedVariant.of(Variant.ofBytes(CompressionUtils.decompressIfNeeded(orderedValue)), orderedValue.getOrder());
                            case INT -> OrderedVariant.of(Variant.ofLong(orderedValue.getValue().getIntPayload()), orderedValue.getOrder());
                            case DOUBLE -> OrderedVariant.of(Variant.ofDouble(orderedValue.getValue().getDoublePayload()), orderedValue.getOrder());
                            case UNRECOGNIZED -> OrderedVariant.of(Variant.none());
                        };
                     }
             )
                .toList());
        value = new ArrayList<>();
    }

    @Override
    public void onNext(BatchValueResponse value) {
        this.value.addAll(function.apply(value));
    }

    public static class BooleanObserver extends CompletableFutureObserver<BoolResponse, List<Boolean>> {

        public BooleanObserver(CompletableFuture<List<Boolean>> future) {
            super(future, res -> List.of(res.getValue()));
        }

        @Override
        public void onNext(BoolResponse value) {
            this.value.addAll(function.apply(value));
        }
    }
}