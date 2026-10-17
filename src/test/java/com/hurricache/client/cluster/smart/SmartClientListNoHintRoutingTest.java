package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for List operations WITHOUT keyHint.
 */
public class SmartClientListNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("Create empty list, read on MASTER without keyHint")
    void testCreateEmptyListWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create empty list, read on BACKUP without keyHint")
    void testCreateEmptyListWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_empty_b_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create list with bytes, read on MASTER without keyHint")
    void testCreateListWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create list with bytes, read on BACKUP without keyHint")
    void testCreateListWithBytesWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_bytes_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create list with int, read on MASTER without keyHint")
    void testCreateListWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create list with int, read on BACKUP without keyHint")
    void testCreateListWithIntWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_int_b_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Add element to list tail on MASTER without keyHint")
    void testAddElementToListTailOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_add_tail_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer added = client.pushTail(key, null, List.of(v("elem1"))).get();
        assertEquals(1, added);

        Thread.sleep(500);
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("Add element to list tail on BACKUP without keyHint")
    void testAddElementToListTailOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_add_tail_b_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        client.pushTail(key, null, List.of(v("elem1"))).get();
        Thread.sleep(500);

        Integer added = client.setMode(Mode.BACKUP).pushTail(key, null, List.of(v("elem2"))).get();
        assertEquals(2, added);
    }

    @Test
    @DisplayName("Add element to list head on MASTER without keyHint")
    void testAddElementToListHeadOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_add_head_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer added = client.pushHead(key, null, List.of(v("elem1"))).get();
        assertEquals(1, added);

        Thread.sleep(500);
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("Get list size on MASTER without keyHint")
    void testListSizeOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"), v("elem3"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Get list size on BACKUP without keyHint")
    void testListSizeOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_size_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"), v("elem3"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Stream list on MASTER without keyHint")
    void testStreamListOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_stream_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getList(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Stream list on BACKUP without keyHint")
    void testStreamListOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_stream_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Get head from list on MASTER without keyHint")
    void testGetHeadFromListOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_head_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("second"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key, null).get();
        assertNotNull(head);
        assertEquals("first", new String(head.bytesValue()));
    }

    @Test
    @DisplayName("Get tail from list on BACKUP without keyHint")
    void testGetTailFromListOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_tail_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("second"));

        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        Variant tail = client.setMode(Mode.BACKUP).peekTail(key, null).get();
        assertNotNull(tail);
        assertEquals("second", new String(tail.bytesValue()));
    }

    // =========================================================================
    // 8. ADD ELEMENT (UNORDERED) WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("addElementUnordered on MASTER without keyHint")
    void testAddElementUnorderedWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "list_no_hint_add_element_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("first"))).get();
        Thread.sleep(500);

        // AddElement on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Integer added = client.addElementUnordered(key, null, List.of(v("elem1"), v("elem2"))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, stream.size());
    }
}