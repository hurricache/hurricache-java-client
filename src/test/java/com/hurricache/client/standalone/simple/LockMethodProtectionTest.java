package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LockMethodProtectionTest extends TestBase {

    private final String testKey = "lock_protected_item";
    private final int ownerId = 1;
    private final int intruderId = 2;

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others - Create on Master")
    void testGlobalLockUnaryProtectionCreateOnMaster() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        assertPermissionDenied(() -> client.getValue(testKey1, intruderId).get());
        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "new".getBytes(StandardCharsets.UTF_8), intruderId).get());
        assertPermissionDenied(() -> client.remove(testKey1, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others - Create on Backup")
    void testGlobalLockUnaryProtectionCreateOnBackup() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get());
        Thread.sleep(500);

        client.lockObject(testKey1, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        assertPermissionDenied(() -> client.getValue(testKey1, intruderId).get());
        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "new".getBytes(StandardCharsets.UTF_8), intruderId).get());
        assertPermissionDenied(() -> client.remove(testKey1, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others - Create on Master")
    void testGlobalLockCollectionProtectionCreateOnMaster() throws Exception {
        String listKey = "global_list" + UUID.randomUUID();

        Assertions.assertNotNull(client.createList(listKey, List.of(Payload.of("item1".getBytes())), ownerId).get());
        Thread.sleep(150);

        client.lockObject(listKey, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        assertPermissionDenied(() -> client.getFront(listKey, intruderId).get());
        assertPermissionDenied(() -> client.addElementToTail(listKey, null, Collections.singletonList(Payload.of("item2".getBytes())), intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others - Create on Backup")
    void testGlobalLockCollectionProtectionCreateOnBackup() throws Exception {
        String listKey = "global_list" + UUID.randomUUID();

        Assertions.assertNotNull(client.createList(listKey, List.of(Payload.of("item1".getBytes())), ownerId).get());
        Thread.sleep(150);

        client.lockObject(listKey, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        assertPermissionDenied(() -> client.getFront(listKey, intruderId).get());
        assertPermissionDenied(() -> client.addElementToTail(listKey, null, Collections.singletonList(Payload.of("item2".getBytes())), intruderId).get());
    }

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write - Create on Master")
    void testWriteLockProtectionCreateOnMaster() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        byte[] data = client.getValue(testKey1, intruderId).get();
        assertNotNull(data);

        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "fail".getBytes(StandardCharsets.UTF_8), intruderId).get());

        byte[] ownerUpdate = client.updateKeyValue(testKey1, "success".getBytes(StandardCharsets.UTF_8), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write - Create on Backup")
    void testWriteLockProtectionCreateOnBackup() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        byte[] data = client.getValue(testKey1, intruderId).get();
        assertNotNull(data);

        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "fail".getBytes(StandardCharsets.UTF_8), intruderId).get());

        byte[] ownerUpdate = client.updateKeyValue(testKey1, "success".getBytes(StandardCharsets.UTF_8), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads - Create on Master")
    void testReadLockProtectionCreateOnMaster() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        assertNotNull(client.getValue(testKey1, intruderId).get());
        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "bad".getBytes(StandardCharsets.UTF_8), intruderId).get());
        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "bad".getBytes(StandardCharsets.UTF_8), ownerId).get());
    }

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads - Create on Backup")
    void testReadLockProtectionCreateOnBackup() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        assertNotNull(client.getValue(testKey1, intruderId).get());
        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "bad".getBytes(StandardCharsets.UTF_8), intruderId).get());
        assertPermissionDenied(() -> client.updateKeyValue(testKey1, "bad".getBytes(StandardCharsets.UTF_8), ownerId).get());
    }

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists - Create on Master")
    void testLockCompatibilityCreateOnMaster() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        LockStatus res = client.lockObject(testKey1, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        LockStatus resRead = client.lockObject(testKey1, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, resRead);
    }

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists - Create on Backup")
    void testLockCompatibilityCreateOnBackup() throws Exception {
        String testKey1 = testKey + UUID.randomUUID();
        Assertions.assertNotNull(client.createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get());
        Thread.sleep(150);

        client.lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        LockStatus res = client.lockObject(testKey1, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        LockStatus resRead = client.lockObject(testKey1, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, resRead);
    }

    private void assertPermissionDenied(Executable runnable) {
        ExecutionException e = assertThrows(ExecutionException.class, runnable);
        StatusRuntimeException grpcEx = (StatusRuntimeException) e.getCause();
        assertEquals(Status.Code.PERMISSION_DENIED,
                grpcEx.getStatus().getCode(),
                "Expected PERMISSION_DENIED but got " + grpcEx.getStatus().getCode());
    }
}