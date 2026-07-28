package com.hurricache.utils;

import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamBatchOrderedObserver extends CompletableFutureObserver<BatchValueResponse, List<OrderedPayload>> {

    public StreamBatchOrderedObserver(CompletableFuture<List<OrderedPayload>> future) {
        super(future, res -> res.getValueOrderedList().stream()
                .map(orderedValue -> OrderedPayload.of(
                        orderedValue.getOrder(),
                        CompressionUtils.decompressIfNeeded(orderedValue)
                ))
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