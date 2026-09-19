# План: QueueOperationsClusterTest — Кластерные тесты для Queue

## 1. Анализ требований

### Ключевые правила:
1. **KeyHintData** — каждый тест использует hint, возвращаемый методом `createQueue`
2. **100ms на репликацию** — после `createQueue` всегда `Thread.sleep(100)`
3. **Проверка репликации** — каждая write операция проверяет репликацию на 2-ю ноду
4. **Pattern write → check**:
   - Write на мастере → проверка на мастере (без задержки) → проверка на бэкапе (с задержкой 100ms)
   - Write на бэкапе → проверка на бэкапе (без задержки) → проверка на мастере (с задержкой 100ms)
5. **Write операции для Queue**: `addElementToTail`, `getAndRemoveFront`, `getAndRemoveTail`, `removeHead`, `remove`, `setTtl`, `lockObject`
6. **Read операции для Queue**: `getFront`, `getHead`, `getTail`, `streamQueue`, `existKey`, `getSize` (всегда 0)
7. **Unsupported методы** (возвращают INTERNAL): `getElementAtPosition`, `getAndRemoveElementAtPosition`, `addElementToPosition`, `removeElementAtPosition`, `addElementToPositionBefore`, `addElementToPositionAfter`

### Empty queue behavior:
- `getHead` / `getFront` возвращает пустой Payload (пустой byte array)
- `getAndRemoveFront` возвращает пустой Payload, НЕ бросает ошибку
- `removeHead` не бросает на пустой очереди

### Write операции:
- `addElementToTail` — добавление в конец очереди
- `getAndRemoveFront` — извлечение из головы (pop front)
- `getAndRemoveTail` — извлечение из хвоста (pop back)
- `removeHead` — удаление из головы без возврата
- `remove` — удаление всего контейнера
- `setTtl` — установка TTL
- `lockObject` — блокировка

### Read операции:
- `getFront` / `getHead` — посмотреть первый элемент
- `getTail` — посмотреть последний элемент
- `streamQueue` — стрим всего содержимого
- `existKey` — проверка наличия ключа
- `getSize` — размер очереди

## 2. Структура тестов

### Секция 1: Create & Basic Operations
| Тест | Описание |
|------|----------|
| `testCreateEmptyQueue` | create empty → getHead на master (empty) → getHead на backup (empty) |
| `testCreateQueueWithInitialData` | create with 3 elements → getAndRemoveFront на master (k1) → getAndRemoveFront на backup (k1) → verify order k1, k2, k3 |
| `testCreateLargeQueueWithChunking` | create 1500 elements → existKey на master (true) → existKey на backup (true) → streamQueue на master (1500) → streamQueue на backup (1500) |

### Секция 2: Empty Queue Edge Cases
| Тест | Описание |
|------|----------|
| `testGetHeadOnEmptyQueue` | create empty → getHead на master (empty byte[]) → getHead на backup (empty byte[]) |
| `testGetAndRemoveFrontOnEmptyQueue` | create empty → getAndRemoveFront на master (empty byte[]) → getAndRemoveFront на backup (empty byte[]) |

### Секция 3: Add Element To Tail (write!)
| Тест | Описание |
|------|----------|
| `testAddElementToTailOnMaster` | create 3 elements → addElementToTail на master → getTail на master (new) → getTail на backup (new) |
| `testAddElementToTailOnBackup` | create 3 elements → addElementToTail на backup → getTail на backup (new) → getTail на master (new) |

### Секция 4: Get And Remove Front (write!)
| Тест | Описание |
|------|----------|
| `testGetAndRemoveFrontOnMaster` | create 3 elements → getAndRemoveFront на master (k1) → getHead на master (k2) → getHead на backup (k2) |
| `testGetAndRemoveFrontOnBackup` | create 3 elements → getAndRemoveFront на backup (k1) → getHead на backup (k2) → getHead на master (k2) |

### Секция 5: Get And Remove Tail (write!)
| Тест | Описание |
|------|----------|
| `testGetAndRemoveTailOnMaster` | create 3 elements → getAndRemoveTail на master (k3) → getTail на master (k2) → getTail на backup (k2) |
| `testGetAndRemoveTailOnBackup` | create 3 elements → getAndRemoveTail на backup (k3) → getTail на backup (k2) → getTail на master (k2) |

### Секция 6: Remove Head (write!)
| Тест | Описание |
|------|----------|
| `testRemoveHeadOnMaster` | create 3 elements → removeHead на master → getHead на master (k2) → getHead на backup (k2) |
| `testRemoveHeadOnBackup` | create 3 elements → removeHead на backup → getHead на backup (k2) → getHead на master (k2) |

### Секция 7: Get Front / Get Head (read)
| Тест | Описание |
|------|----------|
| `testGetFrontOnMaster` | create 3 elements → getFront на master (k1) → getFront на backup (k1) |
| `testGetFrontOnBackup` | create 3 elements → getFront на backup (k1) → getFront на master (k1) |

### Секция 8: Get Tail (read)
| Тест | Описание |
|------|----------|
| `testGetTailOnMaster` | create 3 elements → getTail на master (k3) → getTail на backup (k3) |
| `testGetTailOnBackup` | create 3 elements → getTail на backup (k3) → getTail на master (k3) |

### Секция 9: Stream Queue (read)
| Тест | Описание |
|------|----------|
| `testStreamQueueOnMaster` | create 3 elements → streamQueue на master (3 entries) → streamQueue на backup (3 entries) |
| `testStreamQueueOnBackup` | create 3 elements → streamQueue на backup (3 entries) → streamQueue на master (3 entries) |

### Секция 10: Exist Key & Get Size (read)
| Тест | Описание |
|------|----------|
| `testExistKeyOnMaster` | create queue → existKey на master (true) → existKey на backup (true) → existKey non-existent (false) |
| `testExistKeyOnBackup` | create queue → existKey на backup (true) → existKey на master (true) |
| `testGetSizeReturnsZero` | create queue → getSize на master (0) → getSize на backup (0) |

### Секция 11: Remove Queue (write!)
| Тест | Описание |
|------|----------|
| `testRemoveQueueOnMaster` | create queue → remove на master → existKey на master (false) → existKey на backup (false) |
| `testRemoveQueueOnBackup` | create queue → remove на backup → existKey на backup (false) → existKey на master (false) |

### Секция 12: Set Ttl Operations
| Тест | Описание |
|------|----------|
| `testSetTtlOnMaster` | create queue → setTtl на master → getTtl на master (>0) → getTtl на backup (>0) |
| `testSetTtlOnBackup` | create queue → setTtl на backup → getTtl на backup (>0) → getTtl на master (>0) |

### Секция 13: TTL Expiration
| Тест | Описание |
|------|----------|
| `testTtlExpirationOnMaster` | create queue → setTtl(2000ms) на master → sleep(2500) → getHead на master (NOT_FOUND) → getHead на backup (NOT_FOUND) |
| `testTtlExpirationOnBackup` | create queue → setTtl(2000ms) на backup → sleep(2500) → getHead на backup (NOT_FOUND) → getHead на master (NOT_FOUND) |

### Секция 14: Lock Operations
| Тест | Описание |
|------|----------|
| `testLockObjectOnMaster` | create queue → lockObject WRITE_LOCK на master → getHead на master (owner OK) → getHead на backup (intruder denied) → unlock |
| `testLockObjectOnBackup` | create queue → lockObject WRITE_LOCK на backup → getHead на backup (owner OK) → getHead на master (intruder denied) → unlock |

### Секция 15: Lock Expiration
| Тест | Описание |
|------|----------|
| `testLockExpirationOnMaster` | create queue → lockObject(2000ms) на master → sleep(2500) → getHead на master (intruder OK) → getHead на backup (intruder OK) |
| `testLockExpirationOnBackup` | create queue → lockObject(2000ms) на backup → sleep(2500) → getHead на backup (intruder OK) → getHead на master (intruder OK) |

### Секция 16: Unsupported Methods
| Тест | Описание |
|------|----------|
| `testUnsupportedMethodsOnQueue` | create queue → getElementAtPosition (INTERNAL) → getAndRemoveElementAtPosition (INTERNAL) → addElementToPosition (INTERNAL) → removeElementAtPosition (INTERNAL) |

## 3. Константы
```java
private static final int OWNER_CLIENT_ID = 100;
private static final int INTRUDER_CLIENT_ID = 200;
private static final long REPLICATION_DELAY_MS = 100;
```

## 4. Утилиты
```java
// bytes() наследуется из TestBaseCluster
// assertDenied() наследуется из TestBaseCluster
```

## 5. Файл для создания
| Файл | Действие |
|------|----------|
| `src/test/java/com/hurricache/client/cluster/smart/QueueOperationsTest.java` | **Создать** — кластерные тесты для Queue |

## 6. Верификация
1. **Компиляция:** `mvn test-compile -q` — без ошибок
2. **Запуск тестов:** `mvn test -Dtest=QueueOperationsTest` — все тесты проходят

## 7. Примечания
- `createQueue` без `setMode`
- После create — 100ms на репликацию
- Все write операции требуют `KeyHintData`
- `addElementToTail` принимает `List<Payload>`
- `getAndRemoveFront` / `getAndRemoveTail` возвращают `Payload` (write!)
- `removeHead` не возвращает значение (write!)
- `streamQueue` возвращает `List<Payload>`
- `getSize` всегда возвращает 0 (не реальное количество элементов)
- Все тесты с блокировками вызывают методы с `clientId`
- TTL expiration использует `Thread.sleep(2500)` при TTL=2000ms
- Lock expiration использует `Thread.sleep(2500)` при lock TTL=2000ms
- `assertDenied()` унаследован из `TestBaseCluster`
- Unsupported методы возвращают `Status.Code.INTERNAL`