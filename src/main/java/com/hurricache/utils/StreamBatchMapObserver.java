package com.hurricache.utils;

import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StreamBatchMapObserver extends CompletableFutureObserver<BatchValueResponse, Map<byte[],byte[]>> {

    public StreamBatchMapObserver(CompletableFuture<Map<byte[],byte[]>> future) {
        super(future, res -> {
            int valueUnorderedCount = res.getValueUnorderedCount();
            int keyUnorderedCount = res.getKeyUnorderedCount();
            int size = Math.min(valueUnorderedCount, keyUnorderedCount);
            Map resp = new HashMap(size);
            for (int i = 0; i < size; i++){
                       resp.put(res.getKeyUnordered(i).getPayload().getPayload().toByteArray(), res.getValueUnordered(i).getValue().getPayload().toByteArray());
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