# HurriCache Java Client

HurriCache is a high-performance distributed caching system with a Java client that supports two operating modes: **Standalone** (single-node) and **Smart** (multi-node cluster with replication).

## Overview

HurriCache provides:
- **Scalar operations**: create, get, replace, remove with support for bytes, int, and double data types
- **Container operations**: Queue, List, Vector, Set, HashMap
- **Atomic operations**: exchange, add, sub, and, or, xor, compareAndSet
- **Locking**: distributed locks with configurable duration and lock types
- **Replication**: automatic data replication across cluster nodes

## Operating Modes

### Standalone Mode

Single-node operation without replication. Connects directly to a HurriCache server instance via gRPC.

**Connection:**
- Direct gRPC connection to a single server
- No coordinator or routing required
- Default port: `50000`

```java
import com.hurricache.client.FastCacheAsyncStandaloneClient;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import java.time.Duration;

// Connect to standalone server at 127.0.0.1:50000
// Parameters: host, port, defaultClientId, timeout
FastCacheAsyncStandaloneClient client = new FastCacheAsyncStandaloneClient(
    "127.0.0.1",    // Server host
    50000,          // Server port
    0,              // Default client ID (0 = use built-in ID)
    Duration.ofSeconds(30)  // Request timeout
);

// Scalar operations
KeyHintData hint = client.create("myKey", v("hello")).get();
Variant value = client.get("myKey", hint).get();
client.replace("myKey", hint, v("world")).get();
client.remove("myKey", hint).get();

// Queue operations
List<Variant> initialData = List.of(v("item1"), v("item2"), v("item3"));
KeyHintData queueHint = client.createQueue("myQueue", initialData).get();
client.pushHead("myQueue", queueHint, List.of(v("newHead"))).get();
client.pushTail("myQueue", queueHint, List.of(v("newTail"))).get();
Variant head = client.peekHead("myQueue", queueHint).get();
Variant popped = client.popTail("myQueue", queueHint).get();

// List operations
KeyHintData listHint = client.createList("myList", initialData).get();
client.pushHead("myList", listHint, List.of(v("head"))).get();
client.pushTail("myList", listHint, List.of(v("tail"))).get();
Variant element = client.getAt("myList", listHint, 0).get();
client.insertAt("myList", listHint, List.of(v("inserted")), 1).get();
client.removeAt("myList", listHint, 0).get();

// Vector operations
KeyHintData vectorHint = client.createVector("myVector", initialData).get();
Variant vecElement = client.getAt("myVector", vectorHint, 0).get();
client.insertAt("myVector", vectorHint, List.of(v("new")), 0).get();
client.popAt("myVector", vectorHint, 0).get();
client.removeAt("myVector", vectorHint, 0).get();

// Set operations
KeyHintData setHint = client.createSet("mySet", initialData).get();
client.addElementUnordered("mySet", setHint, List.of(v("newElement"))).get();
boolean contains = client.containsKey("mySet", setHint, "item1".getBytes()).get();
client.removeFromContainerByKeyAndValue("mySet", setHint, ContainerType.SET, List.of(), List.of(v("item1"))).get();
client.removeFromContainerByKey("mySet", setHint, v("item1")).get();

// HashMap operations
Map<Payload, Variant> mapData = new HashMap<>();
mapData.put(p("key1"), v("value1"));
mapData.put(p("key2"), v("value2"));
KeyHintData mapHint = client.createMap("myMap", mapData).get();
client.putAll("myMap", mapHint, List.of(p("key3")), List.of(v("value3"))).get();
Variant mapValue = client.getInContainer("myMap", mapHint, "key1".getBytes()).get();
client.update("myMap", mapHint, "key1".getBytes(), v("updated")).get();
Variant popped = client.popInContainer("myMap", mapHint, "key2".getBytes()).get();
client.remove("myMap", mapHint, "key3".getBytes()).get();

// Atomic operations
KeyHintData atomicHint = client.create("atomicKey", vi(100)).get();
Number result = client.exchange("atomicKey", atomicHint, 200).get();
Number sum = client.add("atomicKey", atomicHint, 50).get();
Number diff = client.sub("atomicKey", atomicHint, 25).get();
Number bitwise = client.and("atomicKey", atomicHint, 0xFF).get();
Number orResult = client.or("atomicKey", atomicHint, 0x0F).get();
Number xorResult = client.xor("atomicKey", atomicHint, 0xAA).get();
AtomicCasRes casResult = client.compareAndSet("atomicKey", atomicHint, 125, 999).get();

// Locking operations
KeyHintData lockHint = client.create("lockedKey", v("value")).get();
client.lock("lockedKey", lockHint, LockType.WRITE, "clientId", 30).get();
Variant lockedValue = client.get("lockedKey", lockHint).get();
client.replace("lockedKey", lockHint, v("new")).get();
client.unlock("lockedKey", lockHint, "clientId").get();

// Shutdown when done
client.shutdown();
```

### Smart Mode (Cluster)

Multi-node cluster operation with automatic replication, failover, and load balancing. Connects to a cluster of HurriCache servers via coordinator nodes.

**Connection:**
- Connects to coordinator node(s) for cluster topology discovery
- Automatic routing to MASTER/BACKUP nodes based on data partitions
- Built-in failover: if coordinator is unavailable, tries next coordinator
- Default coordinator port: `51000` (also supports `:61000`, `:21000`)
- Background thread refreshes routing info every 30 seconds

```java
import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import java.time.Duration;

// Connect to cluster via coordinator node
// Parameters: coordinatorHost, coordinatorPort, defaultClientId, timeout
FastCacheAsyncSmartClient client = new FastCacheAsyncSmartClient(
    "192.168.0.123",  // Coordinator host
    51000,            // Coordinator port
    0,                // Default client ID (0 = use built-in ID)
    Duration.ofSeconds(30)  // Request timeout
);

// Alternative: connect to multiple coordinators for redundancy
// List<String> coordinators = List.of(
//     "192.168.0.123:51000",
//     "192.168.0.124:51000",
//     "192.168.0.125:51000"
// );
// FastCacheAsyncSmartClient client = new FastCacheAsyncSmartClient(
//     coordinators, 0, Duration.ofSeconds(30)
// );

// All operations are identical to Standalone mode,
// but data is automatically replicated across cluster nodes

// Example: Create with automatic replication
KeyHintData hint = client.create("clusterKey", v("replicated")).get();

// Read operations automatically route to appropriate nodes
Variant value = client.get("clusterKey", hint).get();

// Container operations with replication
List<Variant> data = List.of(v("item1"), v("item2"));
KeyHintData queueHint = client.createQueue("clusterQueue", data).get();
client.pushHead("clusterQueue", queueHint, List.of(v("new"))).get();

// Atomic operations work across cluster
KeyHintData atomicHint = client.create("atomic", vi(0)).get();
client.add("atomic", atomicHint, 1).get();

// Locking with cluster coordination
KeyHintData lockHint = client.create("resource", v("locked")).get();
client.lock("resource", lockHint, LockType.WRITE, "clientId", 60).get();
// ... critical section ...
client.unlock("resource", lockHint, "clientId").get();

// Shutdown when done
client.shutdown();
```

## Stress Tests

The project includes comprehensive stress tests for benchmarking and validation. These tests are provided **for reference only** to help understand performance characteristics and operational patterns.

### Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| **Scalar** | create, get, replace, remove | Tests for bytes, int, double, and heterogeneous data types |
| **Queue** | createQueue, pushHead, pushTail, peekHead, peekTail, popHead, popTail, deleteHead, deleteTail | FIFO/LIFO container operations |
| **List** | createList, getList, pushHead, pushTail, peekHead, peekTail, popHead, popTail, insertAt, removeAt | Ordered sequence operations |
| **Vector** | createVector, getVector, getAt, popAt, insertAt, removeAt | Random-access sequence operations |
| **Set** | createSet, getSet, addElementUnordered, containsKey, removeFromContainerByKeyAndValue, removeFromContainerByKey | Unique element collection operations |
| **HashMap** | createMap, getMap, putAll, getInContainer, update, popInContainer, remove, removeFromContainerByKeyAndValue, removeFromContainerByKey | Key-value store operations |
| **Atomic** | exchange, add, sub, and, or, xor, compareAndSet | Lock-free atomic operations |
| **Lock** | lock, unlock | Distributed locking tests |

### Test Configuration

All stress tests use configurable parameters (defined as `static final`):

```java
private static final int WARMUP_ITERATIONS = 10000;   // Warmup phase iterations
private static final int BENCHMARK_ITERATIONS = 100000; // Benchmark phase iterations
private static final int CONCURRENCY = 100;           // Concurrent in-flight requests
private static final int THREAD_COUNT = 4;            // Number of threads
private static final int CONTAINER_SIZE = 10000;      // Elements in container (Queue/List/Vector)
// CONTAINER_SIZE = 1000 for Set/Map
```

### Test Patterns

- **Sequential tests**: Fire-and-forget `CompletableFuture` pattern with `.exceptionally()` error handling
- **Multi-threaded tests**: `CountDownLatch` for synchronized start, concurrent execution across multiple threads
- **Replication delays**: `Thread.sleep(500)` after `create()` operations for cluster mode
- **Sustained load**: `Thread.sleep(1)` every 100 requests for long-running benchmarks

### Running Tests

```bash
# Run all stress tests
mvn test -Dtest=*StressTest

# Run specific test category
mvn test -Dtest=ClientQueueStressTest
mvn test -Dtest=SmartClientQueueStressTest

# Run with AddressSanitizer (ASAN) for memory error detection
# Note: Tests may fail under ASAN due to multi-threaded race conditions - this is expected
```

### Expected Results

Tests report performance metrics:
```
[Queue.peekHead] size=10000, 100000 iters, 2.303s, 43418.44 OPS, 0 errors
[Queue.peekHead MT] size=10000, 25000 iters x 4 threads, 0.812s, 123166.03 OPS, 0 errors
```

- **OPS**: Operations per second
- **errors**: Number of failed operations (should be 0 for healthy cluster)
- Cluster mode may show create() errors (~33%) due to relay/partition behavior - this is normal

## Building

```bash
mvn clean install
```

## License

This project is licensed under the [GNU General Public License v3.0 (GPL-3.0)](LICENSE).

```
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
```