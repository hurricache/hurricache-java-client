package com.hurricache.redisconnector;

import com.hurricache.client.FastCacheAsyncSmartClient;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

/**
 * Shared environment for RESP-connector cluster tests.
 * Expects a running 3-node cluster started from /ext/fastcache/config-cluster{,-1,-2}.yaml
 * with redis_connector ports 6379/6380/6381.
 */
public final class RespTestEnv {

    public static final String HOST = System.getProperty("fastcache.host", "192.168.0.123");
    public static final int[] RESP_PORTS = {6379, 6380, 6381};
    public static final int NODE_COUNT = RESP_PORTS.length;

    public static final List<String> COORDINATORS = List.of(
            HOST + ":51000", HOST + ":61000", HOST + ":21000");

    public static final String BINARY = System.getProperty("fastcache.binary",
            "/ext/fastcache/cmake-build-release/cache-server/server-fastcache-cluster");
    public static final String[] CONFIGS = {
            "/ext/fastcache/config-cluster.yaml",
            "/ext/fastcache/config-cluster-1.yaml",
            "/ext/fastcache/config-cluster-2.yaml"};

    public static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration FAILOVER_TIMEOUT = Duration.ofSeconds(180);
    public static final long POLL_INTERVAL_MS = 250;

    private RespTestEnv() {
    }

    public static Jedis jedis(int nodeIndex) {
        Jedis jedis = new Jedis(HOST, RESP_PORTS[nodeIndex], 5000);
        return jedis;
    }

    public static FastCacheAsyncSmartClient grpcClient() {
        return new FastCacheAsyncSmartClient(COORDINATORS, 0, Duration.ofSeconds(10)) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(5);
            }

            @Override
            public int getDefaultClientId() {
                return 0;
            }
        };
    }

    /**
     * Polls {@code reader} until it returns a value equal to {@code expected} or the timeout
     * elapses. Returns the last observed value (never throws on mismatch).
     */
    public static <T> T pollUntil(Supplier<T> reader, T expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        T last = null;
        while (System.nanoTime() < deadline) {
            try {
                last = reader.get();
            } catch (Exception e) {
                last = null;
            }
            if (expected == null ? last == null : expected.equals(last)) {
                return last;
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return last;
            }
        }
        return last;
    }

    /**
     * Polls {@code reader} for every expected key in rounds until all keys return their
     * expected values or the (single, global) timeout elapses.
     * Returns the list of keys that never converged (empty = success).
     */
    public static java.util.List<String> pollMissing(java.util.Map<String, String> expected,
                                                     java.util.function.Function<String, String> reader,
                                                     Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        java.util.Map<String, String> missing = new java.util.LinkedHashMap<>(expected);
        while (!missing.isEmpty() && System.nanoTime() < deadline) {
            missing.entrySet().removeIf(e -> {
                try {
                    return java.util.Objects.equals(reader.apply(e.getKey()), e.getValue());
                } catch (Exception ex) {
                    return false;
                }
            });
            if (!missing.isEmpty()) {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return new java.util.ArrayList<>(missing.keySet());
    }

    public static boolean waitForPort(String host, int port, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 1000);
                return true;
            } catch (IOException e) {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    public static void killNode(int nodeIndex) throws IOException, InterruptedException {
        Process p = new ProcessBuilder("pkill", "-f", CONFIGS[nodeIndex] + "$").start();
        int rc = p.waitFor();
        if (rc != 0 && rc != 1) {
            throw new IOException("pkill failed with rc=" + rc + " for " + CONFIGS[nodeIndex]);
        }
    }

    public static void startNode(int nodeIndex) throws IOException {
        new ProcessBuilder(BINARY, "-config-file", CONFIGS[nodeIndex])
                .redirectOutput(ProcessBuilder.Redirect.appendTo(new File("/ext/fastcache/cl-" + nodeIndex)))
                .redirectError(ProcessBuilder.Redirect.appendTo(new File("/ext/fastcache/cl-" + nodeIndex)))
                .start();
    }
}
