package com.hurricache.utils;

import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.Value;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StreamBatchOrderedMapObserver extends CompletableFutureObserver<BatchValueResponse, Map<OrderedPayload, Payload>> {

    public StreamBatchOrderedMapObserver(CompletableFuture<Map<OrderedPayload, Payload>> future) {
        super(future, res -> {
            int valueUnorderedCount = res.getValueUnorderedCount();
            int keyOrderedCount = res.getKeyOrderedCount();
            if (valueUnorderedCount != keyOrderedCount) {
                throw new IllegalArgumentException("Ordered-map batch contains unpaired keys and values");
            }
            int size = keyOrderedCount;
            Map<OrderedPayload, Payload> resp = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++){
                OrderedKey keyOrdered = res.getKeyOrdered(i);
                Value valueUnordered = res.getValueUnordered(i);
                resp.put(OrderedPayload.of(keyOrdered.getOrder(), CompressionUtils.decompressIfNeeded(keyOrdered)),
                         Payload.of(CompressionUtils.decompressIfNeeded(valueUnordered)));
            }
            return resp;
        });
        value = new LinkedHashMap<>();
    }

    @Override
    public void onNext(BatchValueResponse value) {
        if (future.isDone()) {
            return;
        }
        try {
            this.value.putAll(function.apply(value));
        } catch (RuntimeException error) {
            future.completeExceptionally(error);
        }
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
