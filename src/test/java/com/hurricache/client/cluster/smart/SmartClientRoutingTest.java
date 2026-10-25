package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.FastCacheAsyncSimpleClient;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing error and relay semantics tests.
 * Uses direct per-node FastCacheAsyncSimpleClient connections (data node ports from
 * docker/cluster-compose config: 50000/60000/20000 on the same host as the coordinators)
 * to address a specific node, which the smart client itself does not expose.
 */
public class SmartClientRoutingTest extends TestBaseCluster {

    private static final List<String> DATA_NODE_TARGETS = List.of(
            "192.168.0.123:50000",
            "192.168.0.123:60000",
            "192.168.0.123:20000"
    );

    private static FastCacheAsyncSimpleClient newNodeClient(String target) {
        String[] hostPort = target.split(":");
        return new FastCacheAsyncSimpleClient(hostPort[0], Integer.parseInt(hostPort[1]), 0, Duration.ofSeconds(30));
    }

    /**
     * Returns the data node target that owns neither MASTER nor BACKUP role
     * for the shard of the given hint (its get fails with FAILED_PRECONDITION).
     */
    private String findWrongNodeTarget(String key, KeyHintData hint) throws InterruptedException {
        for (String target : DATA_NODE_TARGETS) {
            FastCacheAsyncSimpleClient nodeClient = newNodeClient(target);
            try {
                nodeClient.get(bytes(key), hint).get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StatusRuntimeException sre
                    && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                    return target;
                }
                fail("Unexpected error from node " + target + ": " + e.getCause());
            } finally {
                nodeClient.shutdown();
            }
        }
        return null;
    }

    // =========================================================================
    // 1. NON-CREATE OPERATION ON WRONG NODE -> FAILED_PRECONDITION WITH ADDRESS
    // =========================================================================

    @Test
    @DisplayName("get on wrong node returns FAILED_PRECONDITION with route address in message")
    void testGetOnWrongNodeReturnsFailedPrecondition() throws ExecutionException, InterruptedException {
        String key = "routing_wrong_node_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        assertNotNull(hint);
        Thread.sleep(500);

        int succeeded = 0;
        int failedPrecondition = 0;
        List<FastCacheAsyncSimpleClient> nodeClients = new ArrayList<>();
        try {
            for (String target : DATA_NODE_TARGETS) {
                FastCacheAsyncSimpleClient nodeClient = newNodeClient(target);
                nodeClients.add(nodeClient);
                try {
                    Variant value = nodeClient.get(bytes(key), hint).get();
                    assertNotNull(value);
                    assertEquals("value", new String(value.bytesValue(), StandardCharsets.UTF_8));
                    succeeded++;
                } catch (ExecutionException e) {
                    StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
                    assertEquals(Status.Code.FAILED_PRECONDITION, cause.getStatus().getCode());
                    String description = cause.getStatus().getDescription();
                    assertNotNull(description);
                    assertTrue(description.contains(":"),
                               "Expected route address in FAILED_PRECONDITION message but got: " + description);
                    failedPrecondition++;
                }
            }
        } finally {
            nodeClients.forEach(FastCacheAsyncSimpleClient::shutdown);
        }

        // 3-node cluster: the shard MASTER and BACKUP answer, the third node rejects
        assertEquals(2, succeeded);
        assertEquals(1, failedPrecondition);
    }

    // =========================================================================
    // 2. CREATE ON WRONG NODE -> OK + relayed=true
    // =========================================================================

    @Test
    @DisplayName("create on wrong node succeeds with relayed=true and is readable via smart client")
    void testCreateOnWrongNodeIsRelayed() throws ExecutionException, InterruptedException {
        String probeKey = "routing_relay_probe_" + UUID.randomUUID();
        String relayedKey = "routing_relayed_" + UUID.randomUUID();

        // Probe: find a node that does not own the shard of this hint
        KeyHintData hint = client.create(probeKey, v("probe")).get();
        assertNotNull(hint);
        Thread.sleep(500);

        String wrongNodeTarget = findWrongNodeTarget(probeKey, hint);
        assertNotNull(wrongNodeTarget, "Expected a node that rejects the probe key with FAILED_PRECONDITION");

        // Create a new key with the same shard hint directly on the wrong node -> relayed
        FastCacheAsyncSimpleClient wrongNodeClient = newNodeClient(wrongNodeTarget);
        KeyHintData relayedHint;
        try {
            relayedHint = wrongNodeClient.create(bytes(relayedKey), hint, v("relayed_value"), 0).get();
        } finally {
            wrongNodeClient.shutdown();
        }

        assertNotNull(relayedHint);
        assertTrue(relayedHint.isWasRelayed(), "Expected relayed=true when creating on the wrong node");
        Thread.sleep(500);

        // The relayed key is readable through the smart client on both roles
        Variant masterValue = client.setMode(Mode.MASTER).get(relayedKey, relayedHint).get();
        assertNotNull(masterValue);
        assertEquals("relayed_value", new String(masterValue.bytesValue(), StandardCharsets.UTF_8));

        Variant backupValue = client.setMode(Mode.BACKUP).get(relayedKey, relayedHint).get();
        assertNotNull(backupValue);
        assertEquals("relayed_value", new String(backupValue.bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. KEY HINT FROM CREATE IS USABLE ON MASTER AND BACKUP
    // =========================================================================

    @Test
    @DisplayName("KeyHint from create works for get/replace/remove in MASTER and BACKUP modes")
    void testHintFromCreateUsableInBothModes() throws ExecutionException, InterruptedException {
        String key = "routing_hint_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("initial")).get();
        assertNotNull(hint);
        assertTrue(hint.hasWeekHash());
        Thread.sleep(500);

        // get with hint on MASTER and BACKUP
        Variant value = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals("initial", new String(value.bytesValue(), StandardCharsets.UTF_8));

        value = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals("initial", new String(value.bytesValue(), StandardCharsets.UTF_8));

        // replace with hint on MASTER, verify on BACKUP
        client.setMode(Mode.MASTER).replace(key, hint, v("updated")).get();
        Thread.sleep(500);

        value = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals("updated", new String(value.bytesValue(), StandardCharsets.UTF_8));

        // remove with hint on MASTER, verify on both
        Boolean removed = client.setMode(Mode.MASTER).remove(key, hint).get();
        assertTrue(removed);
        Thread.sleep(500);

        assertFalse(client.setMode(Mode.MASTER).containsKey(key, hint).get());
        assertFalse(client.setMode(Mode.BACKUP).containsKey(key, hint).get());
    }
}
