package com.hurricache.utils;

import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BoolResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StreamBatchMapObserver extends CompletableFutureObserver<BatchValueResponse, Map<Payload, Payload>> {
    static protected Logger logger = LogManager.getLogger(CompletableFutureObserver.class);

    public StreamBatchMapObserver(CompletableFuture<Map<Payload, Payload>> future) {
        super(future, res -> {
            int valueUnorderedCount = res.getValueUnorderedCount();
            int keyUnorderedCount = res.getKeyUnorderedCount();
            if (valueUnorderedCount != keyUnorderedCount){
                logger.warn("Response truncated, valueUnorderedCount != keyUnorderedCount, contact support of hurricache");
            }
            int size = Math.min(valueUnorderedCount, keyUnorderedCount);

            Map<Payload, Payload> resp = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                byte[] keyBytes = res.getKeyUnordered(i).getPayload().getPayload().toByteArray();
                byte[] valBytes = res.getValueUnordered(i).getValue().getPayload().toByteArray();

                resp.put(Payload.of(keyBytes), Payload.of(valBytes));
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