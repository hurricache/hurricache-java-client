package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TTL and lock semantics: no-TTL keys, default lock duration, lock expiry boundary.
 */
public class TtlLockSemanticsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. TTL SEMANTICS
    // =========================================================================

    @Test
    @DisplayName("getTtl on key without TTL returns -1, positive after setTtl")
    void testGetTtlOnKeyWithoutTtl() throws ExecutionException, InterruptedException {
        String key = "ttl_sem_no_ttl_" + UUID.randomUUID();

        // Create with explicit null TTL so the server stores the key without expiry
        KeyHintData hint = client.create(bytes(key), null, vi(42), null, OWNER_CLIENT_ID, Duration.ofSeconds(10)).get();
        assertNotNull(hint);

        // Server does not set the ttl field; client maps it to -1
        Long ttl = client.getTtl(key).get();
        assertNotNull(ttl);
        assertEquals(-1L, ttl);

        // After setTtl the remaining TTL is reported in (0, 60000]
        Boolean setResult = client.setTtl(key, 60000).get();
        assertTrue(setResult);

        Long updated = client.getTtl(key).get();
        assertNotNull(updated);
        assertTrue(updated > 0 && updated <= 60000);

        client.remove(key).get();
    }

    // =========================================================================
    // 2. LOCK SEMANTICS
    // =========================================================================

    @Test
    @DisplayName("lockObject without duration - server default (no expiry), owner unlocks")
    void testLockWithoutDuration() throws ExecutionException, InterruptedException {
        String key = "lock_sem_default_duration_" + UUID.randomUUID();

        client.create(key, v("value")).get();

        // Null duration -> lockDuration field is not sent -> server default UINT32_MAX (no expiry)
        LockStatus lock = client.lock(key, null, LockType.WRITE_LOCK, OWNER_CLIENT_ID, null).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can write
        client.replace(key, v("owner_write"), OWNER_CLIENT_ID).get();
        Variant val = client.get(key, OWNER_CLIENT_ID).get();
        assertEquals("owner_write", str(val));

        // Intruder is denied while the lock is held
        assertDenied(client.replace(key, v("intruder"), INTRUDER_CLIENT_ID));
        assertDenied(client.get(key, INTRUDER_CLIENT_ID));

        // Owner releases the lock
        LockStatus unlock = client.unlock(key, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);

        // After unlock the intruder can write
        client.replace(key, v("after_unlock"), INTRUDER_CLIENT_ID).get();
        val = client.get(key, INTRUDER_CLIENT_ID).get();
        assertEquals("after_unlock", str(val));
    }

    @Test
    @DisplayName("Short lock duration - write denied before expiry, allowed after")
    void testShortLockExpiryBoundary() throws ExecutionException, InterruptedException {
        String key = "lock_sem_short_expiry_" + UUID.randomUUID();

        client.create(key, v("value")).get();

        // Lock duration is sent with seconds resolution; 2s keeps the lock alive for ~1-2s
        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Before expiry: owner can write, intruder is denied
        client.replace(key, v("owner"), OWNER_CLIENT_ID).get();
        assertDenied(client.replace(key, v("intruder"), INTRUDER_CLIENT_ID));

        Thread.sleep(3500);

        // After expiry the intruder can write
        client.replace(key, v("intruder"), INTRUDER_CLIENT_ID).get();
        Variant val = client.get(key, INTRUDER_CLIENT_ID).get();
        assertEquals("intruder", str(val));
    }
}
