package com.hurricache.redisconnector;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cross-interface visibility: data written through gRPC must be readable through RESP
 * (and vice versa) from every node of the cluster.
 * Replication is asynchronous, so cross-node reads are verified with polling.
 */
public class RespGrpcCrossAccessTest {

    private FastCacheAsyncSmartClient grpc;
    private String prefix;

    @BeforeEach
    void setUp() throws Exception {
        grpc = RespTestEnv.grpcClient();
        prefix = "cross:" + UUID.randomUUID() + ":";
    }

    @AfterEach
    void tearDown() throws Exception {
        if (grpc != null) {
            grpc.shutdown();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(Variant v) {
        return new String(v.bytesValue(), StandardCharsets.UTF_8);
    }

    private void respDeleteQuietly(String key) {
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                jedis.del(key);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("gRPC create -> RESP GET readable on every node")
    void grpcWriteRespRead() throws Exception {
        String key = prefix + "grpc2resp";
        String value = "value-from-grpc";

        grpc.create(bytes(key), null, Variant.ofBytes(bytes(value)),
                Duration.ofMinutes(5), 0, Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);

        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                String actual = RespTestEnv.pollUntil(() -> jedis.get(key), value,
                        RespTestEnv.CONVERGENCE_TIMEOUT);
                assertEquals(value, actual, "RESP node " + node + " (port "
                        + RespTestEnv.RESP_PORTS[node] + ") does not see gRPC-written key");
            }
        }
        respDeleteQuietly(key);
    }

    @Test
    @DisplayName("RESP SET (via each node) -> gRPC get")
    void respWriteGrpcRead() throws Exception {
        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            String key = prefix + "resp2grpc:" + node;
            String value = "value-via-node-" + node;
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                assertEquals("OK", jedis.set(key, value));
            }
            Variant actual = grpc.get(bytes(key)).get(15, TimeUnit.SECONDS);
            assertNotNull(actual, "gRPC does not see RESP-written key " + key);
            assertEquals(value, str(actual), "gRPC value mismatch for " + key);
            respDeleteQuietly(key);
        }
    }

    @Test
    @DisplayName("RESP DEL -> gRPC reports NOT_FOUND")
    void respDeleteGrpcNotFound() throws Exception {
        String key = prefix + "respdel";
        try (Jedis jedis = RespTestEnv.jedis(0)) {
            jedis.set(key, "to-delete");
        }
        // wait until gRPC sees it, then delete via RESP from another node
        assertNotNull(RespTestEnv.pollUntil(() -> {
            try {
                return grpc.get(bytes(key)).get(15, TimeUnit.SECONDS);
            } catch (Exception e) {
                return null;
            }
        }, Variant.ofBytes(bytes("to-delete")), RespTestEnv.CONVERGENCE_TIMEOUT),
                "gRPC never saw the key before delete");

        try (Jedis jedis = RespTestEnv.jedis(1)) {
            assertEquals(1L, jedis.del(key));
        }

        // delete propagates asynchronously: poll until gRPC reports NOT_FOUND
        Status.Code code = RespTestEnv.pollUntil(() -> {
            try {
                grpc.get(bytes(key)).get(15, TimeUnit.SECONDS);
                return null;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StatusRuntimeException sre) {
                    return sre.getStatus().getCode();
                }
                return null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (TimeoutException e) {
                return null;
            }
        }, Status.Code.NOT_FOUND, RespTestEnv.CONVERGENCE_TIMEOUT);
        assertEquals(Status.Code.NOT_FOUND, code,
                "gRPC still sees the key after RESP DEL propagated");
    }

    @Test
    @DisplayName("gRPC remove -> RESP returns nil on every node")
    void grpcDeleteRespNil() throws Exception {
        String key = prefix + "grpcdel";
        grpc.create(bytes(key), null, Variant.ofBytes(bytes("gone")),
                Duration.ofMinutes(5), 0, Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);

        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                assertEquals("gone", RespTestEnv.pollUntil(() -> jedis.get(key), "gone",
                        RespTestEnv.CONVERGENCE_TIMEOUT));
            }
        }

        assertTrue(grpc.remove(bytes(key)).get());

        for (int node = 0; node < RespTestEnv.NODE_COUNT; node++) {
            try (Jedis jedis = RespTestEnv.jedis(node)) {
                assertNull(RespTestEnv.pollUntil(() -> jedis.get(key), null,
                        RespTestEnv.CONVERGENCE_TIMEOUT),
                        "RESP node " + node + " still sees removed key");
            }
        }
    }

    @Test
    @DisplayName("RESP INCR -> gRPC reads counter value")
    void respIncrGrpcRead() throws Exception {
        String key = prefix + "counter";
        try (Jedis jedis = RespTestEnv.jedis(0)) {
            assertEquals(1L, jedis.incr(key));
            assertEquals(2L, jedis.incr(key));
            assertEquals(7L, jedis.incrBy(key, 5));
        }

        Variant actual = grpc.get(bytes(key)).get(15, TimeUnit.SECONDS);
        assertNotNull(actual);
        long asLong = actual.isInt() ? actual.longValue() : Long.parseLong(str(actual));
        assertEquals(7L, asLong, "gRPC counter value mismatch after RESP INCR/INCRBY");
        respDeleteQuietly(key);
    }

    @Test
    @DisplayName("gRPC int create -> RESP GET / INCR works on it")
    void grpcIntRespIncr() throws Exception {
        String key = prefix + "grpc-counter";
        grpc.create(bytes(key), null, Variant.ofLong(41),
                Duration.ofMinutes(5), 0, Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);

        try (Jedis jedis = RespTestEnv.jedis(0)) {
            String seen = RespTestEnv.pollUntil(() -> jedis.get(key), "41",
                    RespTestEnv.CONVERGENCE_TIMEOUT);
            assertEquals("41", seen, "RESP does not see gRPC-written int key");
            assertEquals(42L, jedis.incr(key));
        }
        respDeleteQuietly(key);
    }

    @Test
    @DisplayName("RESP RPUSH/LRANGE list visible through gRPC")
    void respListGrpcRead() throws Exception {
        String key = prefix + "list";
        List<String> expected = List.of("a", "b", "c");
        try (Jedis jedis = RespTestEnv.jedis(2)) {
            jedis.rpush(key, "a", "b", "c");
            assertEquals(expected, jedis.lrange(key, 0, -1));
        }

        List<Variant> actual = RespTestEnv.pollUntil(() -> {
            try {
                List<Variant> l = grpc.getList(bytes(key), null, 0, Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
                return l != null && l.size() == 3 ? l : null;
            } catch (Exception e) {
                return null;
            }
        }, null, RespTestEnv.CONVERGENCE_TIMEOUT);
        assertNotNull(actual, "gRPC getList did not return 3 elements for RESP-written list");
        assertEquals(expected, actual.stream().map(RespGrpcCrossAccessTest::str).toList());
        respDeleteQuietly(key);
    }

    @Test
    @DisplayName("RESP EXPIRE -> key disappears for gRPC as well")
    void respExpireGrpc() throws Exception {
        String key = prefix + "expiring";
        try (Jedis jedis = RespTestEnv.jedis(1)) {
            jedis.set(key, "short-lived");
            assertEquals(1L, jedis.expire(key, 1));
        }
        Thread.sleep(2500);
        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> grpc.get(bytes(key)).get());
        assertTrue(ex.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.NOT_FOUND,
                ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }
}
