package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for StandaloneClient chunking logic.
 */
public class ChunkingTest extends TestBase {

    @Test
    @DisplayName("Create queue with small data (single chunk)")
    void testCreateQueueSmallDataSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_queue_small_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        Variant head = client.getHead(key).get();
        assertNotNull(head);
        assertEquals("item1", str(head));
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create list with small data (single chunk)")
    void testCreateListSmallDataSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_list_small_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        Variant element = client.getElementAtPosition(key, 0).get();
        assertNotNull(element);
        assertEquals("item1", str(element));
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create vector with small data (single chunk)")
    void testCreateVectorSmallDataSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_vector_small_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        Variant element = client.getElementAtPosition(key, 1).get();
        assertNotNull(element);
        assertEquals("item2", str(element));
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create set with small data (single chunk)")
    void testCreateSetSmallDataSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_set_small_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> streamed = client.streamSet(key).get();
        assertEquals(3, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create queue with empty list")
    void testCreateQueueEmptyData() throws ExecutionException, InterruptedException {
        String key = "chunk_queue_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createQueue(key, List.of()).get();
        assertNotNull(hint);

        Variant head = client.getHead(key).get();
        assertTrue(head.isNone());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create list with empty list")
    void testCreateListEmptyData() throws ExecutionException, InterruptedException {
        String key = "chunk_list_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of()).get();
        assertNotNull(hint);

        Variant head = client.getHead(key).get();
        assertTrue(head.isNone());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create vector with empty list")
    void testCreateVectorEmptyData() throws ExecutionException, InterruptedException {
        String key = "chunk_vector_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of()).get();
        assertNotNull(hint);

        List<Variant> streamed = client.streamVector(key).get();
        assertTrue(streamed.isEmpty());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create set with empty list")
    void testCreateSetEmptyData() throws ExecutionException, InterruptedException {
        String key = "chunk_set_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of()).get();
        assertNotNull(hint);

        List<Variant> streamed = client.streamSet(key).get();
        assertTrue(streamed.isEmpty());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element unordered - single chunk")
    void testAddElementUnorderedSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_add_unordered_small_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = List.of(v("item1"), v("item2"));
        int added = client.addElementUnordered(key, data).get();
        assertEquals(2, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(2, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to tail - single chunk")
    void testAddElementToTailSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_add_tail_small_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = List.of(v("item1"), v("item2"));
        int added = client.addElementToTail(key, data).get();
        assertEquals(2, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(2, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to head - single chunk")
    void testAddElementToHeadSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_add_head_small_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = List.of(v("item1"), v("item2"));
        int added = client.addElementToHead(key, data).get();
        assertEquals(2, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(2, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element unordered - empty list")
    void testAddElementUnorderedEmpty() throws ExecutionException, InterruptedException {
        String key = "chunk_add_unordered_empty_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        int added = client.addElementUnordered(key, List.of()).get();
        assertEquals(0, added);

        List<Variant> streamed = client.streamList(key).get();
        assertTrue(streamed.isEmpty());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to tail - empty list")
    void testAddElementToTailEmpty() throws ExecutionException, InterruptedException {
        String key = "chunk_add_tail_empty_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        int added = client.addElementToTail(key, List.of()).get();
        assertEquals(0, added);
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to head - empty list")
    void testAddElementToHeadEmpty() throws ExecutionException, InterruptedException {
        String key = "chunk_add_head_empty_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        int added = client.addElementToHead(key, List.of()).get();
        assertEquals(0, added);
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to position - single chunk")
    void testAddElementToPositionSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_add_position_small_" + UUID.randomUUID();
        
        List<Variant> initial = List.of(v("initial"));
        KeyHintData hint = client.createList(key, initial).get();

        List<Variant> data = List.of(v("item1"), v("item2"));
        int added = client.addElementToPosition(key, data, 1).get();
        assertEquals(2, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(3, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to position before - single chunk")
    void testAddElementToPositionBeforeSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_add_position_before_small_" + UUID.randomUUID();
        
        List<Variant> initial = List.of(v("pivot"));
        KeyHintData hint = client.createList(key, initial).get();

        List<Variant> data = List.of(v("item1"), v("item2"));
        int added = client.addElementToPositionBefore(key, data, v("pivot")).get();
        assertEquals(2, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(3, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to position after - single chunk")
    void testAddElementToPositionAfterSingleChunk() throws ExecutionException, InterruptedException {
        String key = "chunk_add_position_after_small_" + UUID.randomUUID();
        
        List<Variant> initial = List.of(v("pivot"));
        KeyHintData hint = client.createList(key, initial).get();

        List<Variant> data = List.of(v("item1"), v("item2"));
        int added = client.addElementToPositionAfter(key, data, v("pivot")).get();
        assertEquals(2, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(3, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create queue with large data (multiple chunks)")
    void testCreateQueueLargeDataMultipleChunks() throws ExecutionException, InterruptedException {
        String key = "chunk_queue_large_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[200 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 20; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        Variant head = client.getHead(key).get();
        assertNotNull(head);
        assertArrayEquals(largePayload, head.bytesValue());

        int count = 0;
        while (true) {
            Variant elem = client.getAndDeleteHead(key).get();
            if (elem.isNone()) break;
            count++;
        }
        assertEquals(20, count);
    }

    @Test
    @DisplayName("Create list with large data (multiple chunks)")
    void testCreateListLargeDataMultipleChunks() throws ExecutionException, InterruptedException {
        String key = "chunk_list_large_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[200 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 20; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(20, streamed.size());
        assertArrayEquals(largePayload, streamed.get(0).bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create vector with large data (multiple chunks)")
    void testCreateVectorLargeDataMultipleChunks() throws ExecutionException, InterruptedException {
        String key = "chunk_vector_large_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[200 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 10; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        Variant elem0 = client.getElementAtPosition(key, 0).get();
        Variant elem9 = client.getElementAtPosition(key, 9).get();
        assertNotNull(elem0);
        assertNotNull(elem9);
        assertArrayEquals(largePayload, elem0.bytesValue());
        assertArrayEquals(largePayload, elem9.bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create set with large data (multiple chunks)")
    void testCreateSetLargeDataMultipleChunks() throws ExecutionException, InterruptedException {
        String key = "chunk_set_large_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] largePayload = new byte[200 * 1024];
            for (int j = 0; j < largePayload.length; j++) {
                largePayload[j] = (byte) ((i + j) % 256);
            }
            data.add(Variant.ofBytes(largePayload));
        }

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> streamed = client.streamSet(key).get();
        assertEquals(10, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element unordered - large data (multiple chunks)")
    void testAddElementUnorderedLargeData() throws ExecutionException, InterruptedException {
        String key = "chunk_add_unordered_large_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[100 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 30; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        int added = client.addElementUnordered(key, data).get();
        assertEquals(30, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(30, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to tail - large data (multiple chunks)")
    void testAddElementToTailLargeData() throws ExecutionException, InterruptedException {
        String key = "chunk_add_tail_large_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[100 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 30; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        int added = client.addElementToTail(key, data).get();
        assertEquals(30, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(30, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to head - large data (multiple chunks)")
    void testAddElementToHeadLargeData() throws ExecutionException, InterruptedException {
        String key = "chunk_add_head_large_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[100 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 30; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        int added = client.addElementToHead(key, data).get();
        assertEquals(30, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(30, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Add element to position - large data (multiple chunks)")
    void testAddElementToPositionLargeData() throws ExecutionException, InterruptedException {
        String key = "chunk_add_position_large_" + UUID.randomUUID();
        
        List<Variant> initial = List.of(v("initial"));
        KeyHintData hint = client.createList(key, initial).get();

        List<Variant> data = new ArrayList<>();
        byte[] largePayload = new byte[100 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 20; i++) {
            data.add(Variant.ofBytes(largePayload));
        }

        int added = client.addElementToPosition(key, data, 1).get();
        assertEquals(20, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(21, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Create queue with compressible data")
    void testCreateQueueWithCompressibleData() throws ExecutionException, InterruptedException {
        String key = "chunk_queue_compressible_" + UUID.randomUUID();
        
        String repetitive = "A".repeat(1000);
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(Variant.ofBytes(repetitive.getBytes(StandardCharsets.UTF_8)));
        }

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        // Verify by draining the queue with getAndDeleteHead
        int count = 0;
        while (true) {
            Variant elem = client.getAndDeleteHead(key).get();
            if (elem.isNone()) break;
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    @DisplayName("Add element with compressible data")
    void testAddElementCompressibleData() throws ExecutionException, InterruptedException {
        String key = "chunk_add_compressible_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        String repetitive = "B".repeat(1000);
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(Variant.ofBytes(repetitive.getBytes(StandardCharsets.UTF_8)));
        }

        int added = client.addElementToTail(key, data).get();
        assertEquals(10, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(10, streamed.size());
        client.remove(key).get();
     }

    @Test
    @DisplayName("Create queue with mixed size elements")
    void testCreateQueueMixedSizes() throws ExecutionException, InterruptedException {
        String key = "chunk_queue_mixed_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        data.add(v("small"));
        data.add(Variant.ofBytes(new byte[50 * 1024]));
        data.add(v("tiny"));
        data.add(Variant.ofBytes(new byte[50 * 1024]));
        data.add(v("medium"));

        KeyHintData hint = client.createQueue(key, data).get();
        assertNotNull(hint);

        // Verify by draining the queue with getAndDeleteHead
        int count = 0;
        while (true) {
            Variant elem = client.getAndDeleteHead(key).get();
            if (elem.isNone()) break;
            count++;
        }
        assertEquals(5, count);
    }

    @Test
    @DisplayName("Add element with mixed size elements")
    void testAddElementMixedSizes() throws ExecutionException, InterruptedException {
        String key = "chunk_add_mixed_" + UUID.randomUUID();
        
        KeyHintData hint = client.createList(key, List.of()).get();

        List<Variant> data = new ArrayList<>();
        data.add(v("small"));
        data.add(Variant.ofBytes(new byte[50 * 1024]));
        data.add(v("tiny"));

        int added = client.addElementUnordered(key, data).get();
        assertEquals(3, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(3, streamed.size());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Multiple sequential chunked operations")
    void testMultipleSequentialChunkedOperations() throws ExecutionException, InterruptedException {
        String key = "chunk_sequential_" + UUID.randomUUID();
        
        List<Variant> initial = new ArrayList<>();
        byte[] payload1 = new byte[100 * 1024];
        for (int i = 0; i < payload1.length; i++) {
            payload1[i] = (byte) (i % 256);
        }
        for (int i = 0; i < 10; i++) {
            initial.add(Variant.ofBytes(payload1));
        }

        KeyHintData hint = client.createList(key, initial).get();
        assertNotNull(hint);

        List<Variant> more = new ArrayList<>();
        byte[] payload2 = new byte[100 * 1024];
        for (int i = 0; i < payload2.length; i++) {
            payload2[i] = (byte) ((i + 128) % 256);
        }
        for (int i = 0; i < 10; i++) {
            more.add(Variant.ofBytes(payload2));
        }

        int added = client.addElementToTail(key, more).get();
        assertEquals(10, added);

        List<Variant> streamed = client.streamList(key).get();
        assertEquals(20, streamed.size());
        
        for (int i = 0; i < 10; i++) {
            assertArrayEquals(payload1, streamed.get(i).bytesValue());
        }
        for (int i = 10; i < 20; i++) {
            assertArrayEquals(payload2, streamed.get(i).bytesValue());
        }
        
        client.remove(key).get();
    }
}