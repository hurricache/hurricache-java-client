package com.hurricache.utils;

import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StreamBatchOrderedMapObserver extends CompletableFutureObserver<BatchValueResponse, Map<OrderedPayload, Payload>> {

    public StreamBatchOrderedMapObserver(CompletableFuture<Map<OrderedPayload, Payload>> future) {
        super(future, res -> {
            int valueUnorderedCount = res.getValueUnorderedCount();
            int keyOrderedCount = res.getKeyOrderedCount();
            int size = Math.min(valueUnorderedCount, keyOrderedCount);
            Map resp = new HashMap(size);
            for (int i = 0; i < size; i++){
                OrderedKey keyOrdered = res.getKeyOrdered(i);
                Value valueUnordered = res.getValueUnordered(i);
                resp.put(OrderedPayload.of(keyOrdered.getOrder(),keyOrdered.getPayload().toByteArray()),Payload.of(valueUnordered.getValue().getPayload().toByteArray()));
            }
            return resp;
        });
        value = new HashMap<>();
    }

    @Override
    public void onNext(BatchValueResponse value) {
        this.value.putAll(function.apply(value));
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