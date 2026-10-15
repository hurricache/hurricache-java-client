package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Variant;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class RawValuesTest extends TestBaseCluster {


    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void singleCreateValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData hint = client.setMode(Mode.MASTER).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        byte[] bytesM = client.setMode(Mode.MASTER).getValue(testKey, hint).get().bytesValue();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(Mode.BACKUP).getValue(testKey, hint).get().bytesValue();
        Assertions.assertNotNull(hint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertArrayEquals(bytesM, bytesB);
    }

    @Test
    void singleCreateValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData hint = client.setMode(Mode.BACKUP).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        byte[] bytesB = client.setMode(Mode.BACKUP).getValue(testKey, hint).get().bytesValue();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(Mode.MASTER).getValue(testKey, hint).get().bytesValue();
        Assertions.assertNotNull(hint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertArrayEquals(bytesM, bytesB);
    }

    @Test
    void singleCreateExistValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        byte[] bytesM = client.setMode(Mode.MASTER).getValue(testKey, keyHint).get().bytesValue();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(Mode.BACKUP).getValue(testKey, keyHint).get().bytesValue();
        Boolean isExistM = client.setMode(Mode.MASTER).existKey(testKey, keyHint).get();
        Boolean isExistB = client.setMode(Mode.BACKUP).existKey(testKey, keyHint).get();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistM);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(Mode.MASTER).getValue(testKey, keyHint).get().bytesValue();
        byte[] bytesB = client.setMode(Mode.BACKUP).getValue(testKey, keyHint).get().bytesValue();
        Boolean isExistM = client.setMode(Mode.MASTER).existKey(testKey, keyHint).get();
        Boolean isExistB = client.setMode(Mode.BACKUP).existKey(testKey, keyHint).get();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistM);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistHintValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        byte[] bytesM = client.setMode(Mode.MASTER).getValue(testKey, keyHint).get().bytesValue();
        Boolean isExistM = client.setMode(Mode.MASTER).existKey(testKey, keyHint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(Mode.BACKUP).getValue(testKey, keyHint).get().bytesValue();
        Boolean isExistB = client.setMode(Mode.BACKUP).existKey(testKey, keyHint).get();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertTrue(isExistM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistHintValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.BACKUP).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(Mode.MASTER).getValue(testKey, keyHint).get().bytesValue();
        Boolean isExistM = client.setMode(Mode.MASTER).existKey(testKey, keyHint).get();
        byte[] bytesB = client.setMode(Mode.BACKUP).getValue(testKey).get().bytesValue();
        Boolean isExistB = client.setMode(Mode.BACKUP).existKey(testKey, keyHint).get();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertTrue(isExistM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateGetAndDeleteValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(Mode.BACKUP).getAndDeleteValue(testKey, keyHint).get().bytesValue();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytesB);
        try {
            client.setMode(Mode.MASTER).getAndDeleteValue(testKey, keyHint).get().bytesValue();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
        try {
            client.setMode(Mode.BACKUP).getAndDeleteValue(testKey, keyHint).get().bytesValue();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateGetAndDeleteValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        byte[] bytes = client.getAndDeleteValue(testKey, keyHint).get().bytesValue();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytes);
        try {
            client.getValue(testKey).get().bytesValue();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValueMaster() throws InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        try {
            client.setMode(Mode.MASTER).getValue(testKey).get().bytesValue();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValueBackup() throws InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        try {
            client.setMode(Mode.BACKUP).getValue(testKey).get().bytesValue();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValueMaster() throws InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        try {
            client.setMode(Mode.MASTER).existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValueBackup() throws InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        try {
            client.setMode(Mode.BACKUP).existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateUpdateValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayloadKey(KEY_SIZE);
        byte[] testValue = createLargePayloadKey(VALUE_SIZE);
        byte[] testValueUpdate = createLargePayloadKey(VALUE_SIZE);
        KeyHintData keyHint = client.setMode(Mode.MASTER).createKeyValue(testKey, Variant.ofBytes(testValue)).get();
        byte[] bytes = client.setMode(Mode.MASTER).getValue(testKey, keyHint).get().bytesValue();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytes);
        Variant oldVal = client.setMode(Mode.MASTER).updateKeyValue(testKey, keyHint, Variant.ofBytes(testValueUpdate)).get();
        Variant newValM = client.setMode(Mode.MASTER).getValue(testKey, keyHint).get();
        Assertions.assertArrayEquals(testValue, oldVal.bytesValue());
        Assertions.assertArrayEquals(testValueUpdate, newValM.bytesValue());
        Thread.sleep(100);
        byte[] newValB = client.setMode(Mode.BACKUP).getValue(testKey, keyHint).get().bytesValue();
        Assertions.assertArrayEquals(testValue, oldVal.bytesValue());
        Assertions.assertArrayEquals(testValueUpdate, newValB);
    }
}