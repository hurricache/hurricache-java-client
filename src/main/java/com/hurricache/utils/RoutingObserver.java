package com.hurricache.utils;

import com.hurricache.grpc.coordinator.PeerRouting;
import com.hurricache.grpc.coordinator.RoutingInfoData;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RoutingObserver extends CompletableFutureObserver<RoutingInfoData, List<PeerRouting>> {

    private int maxShards;

    public RoutingObserver(CompletableFuture<List<PeerRouting>> future) {
        super(future, res -> res.getPeerRoutingList());
        value = new ArrayList<>();
    }

    @Override
    public void onNext(RoutingInfoData value) {
        maxShards = Math.max(maxShards, value.getMaxShards());
        this.value.addAll(function.apply(value));
    }

    public int getMaxShards() {
        return maxShards;
    }
}