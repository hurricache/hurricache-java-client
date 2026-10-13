package com.hurricache.utils;

import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamBatchUnorderedObserver extends CompletableFutureObserver<BatchValueResponse, List<Variant>> {

    public StreamBatchUnorderedObserver(CompletableFuture<List<Variant>> future) {
        super(future, res -> res.getValueUnorderedList().stream()
                .map(val -> {
                    byte[] decompressed = CompressionUtils.decompressIfNeeded(val);
                    return Variant.ofBytes(decompressed);
                })
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