package com.hurricache.redisconnector;

import com.hurricache.client.FastCacheAsyncSmartClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Failover: data written through RESP must survive the loss of a cluster node
 * (replicated to backup), stay readable/writable while the node is down,
 * and the rejoined node must catch up.
 * <p>
 * The test kills node 2 (config-cluster-2.yaml) and restarts it afterwards.
 * Every verification phase is bounded by a single global deadline (not per-key),
 * so a systematic failure fails fast instead of hanging the suite.
 */
public class RespFailoverTest {

    private static final int VICTIM_NODE = 2;
    private static final int KEY_COUNT = 90;

    private String prefix;
    private boolean victimKilled = false;

    @BeforeEach
    void setUp() {
        prefix = "failover:" + UUID.randomUUID() + ":";
    }

    @AfterEach
    void tearDown() throws Exception {
        if (victimKilled) {
            RespTestEnv.startNode(VICTIM_NODE);
            RespTestEnv.waitForPort(RespTestEnv.HOST, RespTestEnv.RESP_PORTS[VICTIM_NODE],
                    Duration.ofSeconds(30));
            victimKilled = false;
        }
    }

    private Map<String, String> writeKeys(String keyPrefix, int count, int[] ports) {
        Map<String, String> expected = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String key = keyPrefix + i;
            String value = "val" + i;
            try (Jedis jedis = RespTestEnv.jedis(ports[i % ports.length])) {
                assertEquals("OK", jedis.set(key, value));
            }
            expected.put(key, value);
        }
        return expected;
    }

    private void assertConvergedOn(String phase, int node, Map<String, String> expected, Duration timeout) {
        try (Jedis jedis = RespTestEnv.jedis(node)) {
            List<String> missing = RespTestEnv.pollMissing(expected, jedis::get, timeout);
            assertTrue(missing.isEmpty(),
                    phase + ": node " + node + " (port " + RespTestEnv.RESP_PORTS[node] + ") missing "
                            + missing.size() + "/" + expected.size() + " keys, e.g. "
                            + missing.subList(0, Math.min(5, missing.size())));
        }
    }

    @Test
    @DisplayName("Data survives node kill (replica on backup), new node catches up after rejoin")
    void dataSurvivesNodeKill() throws Exception {
        int[] allNodes = {0, 1, 2};
        int[] survivors = {0, 1};

        Map<String, String> keys = writeKeys(prefix + "k", KEY_COUNT, allNodes);
        for (int node : allNodes) {
            assertConvergedOn("initial convergence", node, keys, RespTestEnv.CONVERGENCE_TIMEOUT);
        }

        // kill the victim node: every key must remain readable through the survivors
        RespTestEnv.killNode(VICTIM_NODE);
        victimKilled = true;
        for (int node : survivors) {
            assertConvergedOn("after kill", node, keys, RespTestEnv.FAILOVER_TIMEOUT);
        }

        // writes while the node is down must work and survive
        Map<String, String> lateKeys = writeKeys(prefix + "late", 30, survivors);
        for (int node : survivors) {
            assertConvergedOn("late writes", node, lateKeys, RespTestEnv.CONVERGENCE_TIMEOUT);
        }

        // gRPC must see RESP-written data after failover as well
        FastCacheAsyncSmartClient grpc = RespTestEnv.grpcClient();
        try {
            List<String> missing = RespTestEnv.pollMissing(keys, k -> {
                try {
                    var v = grpc.get(k.getBytes(StandardCharsets.UTF_8)).get(15, TimeUnit.SECONDS);
                    return v == null ? null : new String(v.bytesValue(), StandardCharsets.UTF_8);
                } catch (Exception e) {
                    return null;
                }
            }, RespTestEnv.FAILOVER_TIMEOUT);
            assertTrue(missing.isEmpty(),
                    "gRPC lost " + missing.size() + "/" + keys.size() + " keys after failover, e.g. "
                            + missing.subList(0, Math.min(5, missing.size())));
        } finally {
            grpc.shutdown();
        }

        // bring the node back: it must rejoin and catch up with everything written
        RespTestEnv.startNode(VICTIM_NODE);
        victimKilled = false;
        assertTrue(RespTestEnv.waitForPort(RespTestEnv.HOST, RespTestEnv.RESP_PORTS[VICTIM_NODE],
                Duration.ofSeconds(30)), "restarted node RESP port did not come up");

        Map<String, String> all = new LinkedHashMap<>(keys);
        all.putAll(lateKeys);
        assertConvergedOn("rejoin catch-up", VICTIM_NODE, all, RespTestEnv.FAILOVER_TIMEOUT);
    }
}
