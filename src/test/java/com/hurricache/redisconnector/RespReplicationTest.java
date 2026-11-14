package com.hurricache.redisconnector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Replication convergence: everything written through RESP on any node must become
 * visible through RESP on every other node (asynchronous replication -> poll).
 * Verification phases use a single global deadline each (fail-fast on systematic failure).
 */
public class RespReplicationTest {

    private static final int KEY_COUNT = 60;

    private String prefix;

    @BeforeEach
    void setUp() {
        prefix = "repl:" + UUID.randomUUID() + ":";
    }

    private Map<String, String> writeKeys(int count, int startPort) {
        Map<String, String> expected = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String key = prefix + "k" + i;
            String value = "val" + i;
            try (Jedis jedis = RespTestEnv.jedis((startPort + i) % RespTestEnv.NODE_COUNT)) {
                assertEquals("OK", jedis.set(key, value));
            }
            expected.put(key, value);
        }
        return expected;
    }

    private void assertConverged(String phase, int node, Map<String, String> expected) {
        try (Jedis jedis = RespTestEnv.jedis(node)) {
            List<String> missing = RespTestEnv.pollMissing(expected, jedis::get,
                    RespTestEnv.CONVERGENCE_TIMEOUT);
            assertTrue(missing.isEmpty(),
                    phase + ": node " + node + " (port " + RespTestEnv.RESP_PORTS[node] + ") missing "
                            + missing.size() + "/" + expected.size() + " keys, e.g. "
                            + missing.subList(0, Math.min(5, missing.size())));
        }
    }

    @Test
    @DisplayName("Writes via all nodes converge on every node")
    void writesConvergeOnAllNodes() {
        Map<String, String> keys = writeKeys(KEY_COUNT, 0);
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            assertConverged("writes", node, keys);
        }
    }

    @Test
    @DisplayName("Update via another node converges everywhere")
    void updatesConverge() {
        String key = prefix + "upd";
        try (Jedis jedis = RespTestEnv.jedis(0)) {
            jedis.set(key, "first");
        }
        // overwrite from a different node
        try (Jedis jedis = RespTestEnv.jedis(1)) {
            jedis.set(key, "second");
        }
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                String actual = RespTestEnv.pollUntil(() -> jedis.get(key), "second",
                        RespTestEnv.CONVERGENCE_TIMEOUT);
                assertEquals("second", actual,
                        "node " + node + " did not converge to updated value");
            }
        }
    }

    @Test
    @DisplayName("Delete via another node converges everywhere")
    void deletesConverge() {
        Map<String, String> keys = writeKeys(10, 0);
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            assertConverged("before delete", node, keys);
        }
        try (Jedis jedis = RespTestEnv.jedis(2)) {
            for (String key : keys.keySet()) {
                jedis.del(key);
            }
        }
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                Map<String, String> ghosts = new LinkedHashMap<>();
                for (String key : keys.keySet()) {
                    ghosts.put(key, null);
                }
                List<String> stillThere = RespTestEnv.pollMissing(ghosts, jedis::get,
                        RespTestEnv.CONVERGENCE_TIMEOUT);
                assertTrue(stillThere.isEmpty(),
                        "node " + node + " still sees " + stillThere.size() + " deleted keys, e.g. "
                                + stillThere.subList(0, Math.min(5, stillThere.size())));
            }
        }
    }

    @Test
    @DisplayName("INCR storm from different nodes keeps final value")
    void incrConverges() {
        String key = prefix + "counter";
        int perNode = 20;
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                for (int i = 0; i < perNode; i++) {
                    jedis.incr(key);
                }
            }
        }
        long expected = (long) perNode * RespTestEnv.NODE_COUNT;
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                String actual = RespTestEnv.pollUntil(() -> jedis.get(key),
                        String.valueOf(expected), RespTestEnv.CONVERGENCE_TIMEOUT);
                assertEquals(String.valueOf(expected), actual,
                        "node " + node + " counter mismatch");
            }
        }
    }
}
