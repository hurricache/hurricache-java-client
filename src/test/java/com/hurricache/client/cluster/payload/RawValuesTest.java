package com.hurricache.client.cluster.payload;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.grpc.KeyHint;
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
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint hint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, hint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey, hint).get();
        Assertions.assertNotNull(hint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertArrayEquals(bytesM, bytesB);
    }

    @Test
    void singleCreateValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint hint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey, hint).get();
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, hint).get();
        Assertions.assertNotNull(hint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertArrayEquals(bytesM, bytesB);
    }

    @Test
    void singleCreateExistValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, KeyHint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey, KeyHint).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey, KeyHint).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistM);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey, testValue).get();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, KeyHint).get();
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey, KeyHint).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey, KeyHint).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistM);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistHintValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,KeyHint).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey, KeyHint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,KeyHint).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertTrue(isExistM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistHintValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, keyHint).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey, keyHint).get();
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,keyHint).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey, keyHint).get();
        Assertions.assertNotNull(keyHint);
        Assertions.assertArrayEquals(testValue, bytesM);
        Assertions.assertTrue(isExistM);
        Assertions.assertArrayEquals(testValue, bytesB);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateGetAndDeleteValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndDeleteValue(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertArrayEquals(testValue, bytesB);
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndDeleteValue(testKey, KeyHint).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndDeleteValue(testKey, KeyHint).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateGetAndDeleteValueBackup() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        KeyHint keyhint = client.createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytes = client.getAndDeleteValue(testKey,keyhint).get();
        Assertions.assertNotNull(keyhint);
        Assertions.assertArrayEquals(testValue, bytes);
        try {
            client.getValue(testKey,keyhint).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValueMaster() throws InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValueBackup() throws InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValueMaster() throws InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValueBackup() throws InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateUpdateValueMaster() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        byte[] testValueUpdate = createLargePayload(VALUE_SIZE);
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertArrayEquals(testValue, bytes);
        byte[] oldVal = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey,KeyHint, testValueUpdate).get();
        byte[] newValM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey, KeyHint).get();
        Assertions.assertArrayEquals(testValue, oldVal);
        Assertions.assertArrayEquals(testValueUpdate, newValM);
        Thread.sleep(100);
        byte[] newValB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey, KeyHint).get();
        Assertions.assertArrayEquals(testValue, oldVal);
        Assertions.assertArrayEquals(testValueUpdate, newValB);
    }


    @Test
    void singleCreateUpdateValueMasterNokeyhint() throws ExecutionException, InterruptedException {
        byte[] testKey = createLargePayload(KEY_SIZE);
        byte[] testValue = createLargePayload(VALUE_SIZE);
        byte[] testValueUpdate = createLargePayload(VALUE_SIZE);
        client.createKeyValue(testKey, testValue).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertArrayEquals(testValue, bytes);
        byte[] oldVal = client.updateKeyValue(testKey, testValueUpdate).get();
        byte[] newValM = client.getValue(testKey).get();
        Assertions.assertArrayEquals(testValue, oldVal);
        Assertions.assertArrayEquals(testValueUpdate, newValM);
        Thread.sleep(100);
        byte[] newValB = client.getValue(testKey).get();
        Assertions.assertArrayEquals(testValue, oldVal,new String(testValue) + " vs "+new String(oldVal));
        Assertions.assertArrayEquals(testValueUpdate, newValB);
    }
}