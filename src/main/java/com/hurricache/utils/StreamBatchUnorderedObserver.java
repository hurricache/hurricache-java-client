package com.hurricache.utils;

import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamBatchUnorderedObserver extends CompletableFutureObserver<BatchValueResponse, List<Payload>> {

    public StreamBatchUnorderedObserver(CompletableFuture<List<Payload>> future) {
        super(future, res -> res.getValueUnorderedList().stream()
                .map(val -> Payload.of(CompressionUtils.decompressIfNeeded(val)))
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