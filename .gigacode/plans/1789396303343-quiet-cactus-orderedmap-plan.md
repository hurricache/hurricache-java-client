# План: OrderedMapOperationsClusterTest — Кластерные тесты для OrderedMap

## 1. Анализ требований

### Ключевые правила (как для RawValuesClusterTest):
1. **KeyHintData** — каждый тест использует hint, возвращаемый методом `createOrderedMap`
2. **100ms на репликацию** — после `createOrderedMap` всегда `Thread.sleep(100)`
3. **Проверка репликации** — каждая write операция проверяет репликацию на 2-ю ноду
4. **Pattern write → check**:
   - Write на мастере → проверка на мастере (без задержки) → проверка на бэкапе (с задержкой 100ms)
   - Write на бэкапе → проверка на бэкапе (без задержки) → проверка на мастере (с задержкой 100ms)
5. **Write операции для OrderedMap**: `addElementOrderedMap`, `updateContainerValue`, `removeFromContainer`, `removeElementAtPosition`, `setTtl`, `lockObject`
6. **Отдельные тесты** для setTtl и lock на мастере и на бэкапе
7. **TTL expiration** — проверить истечение на мастере и на бэкапе
8. **Lock expiration** — проверить истечение на мастере и на бэкапе

### Write операции:
- `addElementOrderedMap` — добавление элементов в OrderedMap
- `updateContainerValue` — обновление значения элемента
- `removeFromContainer` — удаление элемента по ключу
- `removeElementAtPosition` — удаление по weight range
- `getAndRemoveContainerValue` — извлечение + удаление элемента (write!)
- `remove` — удаление всего контейнера
- `setTtl` — установка TTL
- `lockObject` — блокировка

### Read операции:
- `getSize` — размер контейнера
- `getContainerValue` — получение значения
- `streamOrderedMap` — стрим всего содержимого
- `streamElementInRangeOrderedMap` — стрим в диапазоне весов
- `containsContainerKey` — проверка наличия ключа

## 2. Структура тестов

### Секция 1: Create & Basic Operations (исправить существующие)
| Тест | Описание |
|------|----------|
| `testCreateEmptyOrderedMap` | create empty → getSize на master (0) → getSize на backup (0) |
| `testCreateOrderedMapWithData` | create with 3 elements → getSize на master (3) → getSize на backup (3) → getContainerValue k1/k2/k3 на master → getContainerValue k1/k2/k3 на backup |
| `testCreateLargeOrderedMapWithChunking` | create 1500 elements → getSize на master (1500) → getSize на backup (1500) → getContainerValue key_0/value_0 на master → getContainerValue key_0/value_0 на backup → getContainerValue key_1499/value_1499 на master → getContainerValue key_1499/value_1499 на backup |

### Секция 2: addElementOrderedMap operations
| Тест | Описание |
|------|----------|
| `testAddElementOrderedMapOnMaster` | create → addElement на master → getSize на master → getSize на backup |
| `testAddElementOrderedMapOnBackup` | create → addElement на backup → getSize на backup → getSize на master |

### Секция 3: updateContainerValue operations
| Тест | Описание |
|------|----------|
| `testUpdateContainerValueOnMaster` | create → updateContainerValue на master → getContainerValue на master → getContainerValue на backup |
| `testUpdateContainerValueOnBackup` | create → updateContainerValue на backup → getContainerValue на backup → getContainerValue на master |

### Секция 4: removeFromContainer operations
| Тест | Описание |
|------|----------|
| `testRemoveFromContainerOnMaster` | create → removeFromContainer на master → getSize на master → getSize на backup |
| `testRemoveFromContainerOnBackup` | create → removeFromContainer на backup → getSize на backup → getSize на master |

### Секция 5: containsContainerKey operations
| Тест | Описание |
|------|----------|
| `testContainsContainerKeyOnMaster` | create → containsContainerKey на master → containsContainerKey на backup |
| `testContainsContainerKeyOnBackup` | create → containsContainerKey на backup → containsContainerKey на master |

### Секция 6: removeElementAtPosition operations
| Тест | Описание |
|------|----------|
| `testRemoveElementAtPositionOnMaster` | create → addElement → removeElementAtPosition на master → getSize на master → getSize на backup |
| `testRemoveElementAtPositionOnBackup` | create → addElement → removeElementAtPosition на backup → getSize на backup → getSize на master |

### Секция 7: streamElementInRangeOrderedMap operations
| Тест | Описание |
|------|----------|
| `testStreamElementInRangeOnMaster` | create → addElement → streamElementInRange на master → streamElementInRange на backup |
| `testStreamElementInRangeOnBackup` | create → addElement → streamElementInRange на backup → streamElementInRange на master |

### Секция 8: getAndRemoveContainerValue operations (write!)
| Тест | Описание |
|------|----------|
| `testGetAndRemoveContainerValueOnMaster` | create → getAndRemoveContainerValue на master → getContainerValue на master (NOT_FOUND) → getContainerValue на backup (NOT_FOUND) |
| `testGetAndRemoveContainerValueOnBackup` | create → getAndRemoveContainerValue на backup → getContainerValue на backup (NOT_FOUND) → getContainerValue на master (NOT_FOUND) |

### Секция 9: remove container operations (write!)
| Тест | Описание |
|------|----------|
| `testRemoveContainerOnMaster` | create → remove container на master → getContainerValue на master (NOT_FOUND) → getContainerValue на backup (NOT_FOUND) |
| `testRemoveContainerOnBackup` | create → remove container на backup → getContainerValue на backup (NOT_FOUND) → getContainerValue на master (NOT_FOUND) |

### Секция 10: getSize operations
| Тест | Описание |
|------|----------|
| `testGetSizeOnMaster` | create → getSize на master → getSize на backup |
| `testGetSizeOnBackup` | create → getSize на backup → getSize на master |

### Секция 11: setTtl operations
| Тест | Описание |
|------|----------|
| `testSetTtlOnMaster` | create → setTtl на master → getTtl на master → getTtl на backup |
| `testSetTtlOnBackup` | create → setTtl на backup → getTtl на backup → getTtl на master |

### Секция 12: TTL expiration
| Тест | Описание |
|------|----------|
| `testTtlExpirationOnMaster` | create → setTtl(2000ms) на master → getTtl на master → getTtl на backup → sleep(3000) → getContainerValue NOT_FOUND на master → NOT_FOUND на backup |
| `testTtlExpirationOnBackup` | create → setTtl(2000ms) на backup → getTtl на backup → getTtl на master → sleep(3000) → getContainerValue NOT_FOUND на backup → NOT_FOUND на master |

### Секция 13: Lock operations
| Тест | Описание |
|------|----------|
| `testLockObjectOnMaster` | create → lockObject WRITE_LOCK на master → getContainerValue на master → getContainerValue на backup (intruder denied) → unlock |
| `testLockObjectOnBackup` | create → lockObject WRITE_LOCK на backup → getContainerValue на backup → getContainerValue на master (intruder denied) → unlock |

### Секция 14: Lock expiration
| Тест | Описание |
|------|----------|
| `testLockExpirationOnMaster` | create → lockObject(2000ms) на master → getContainerValue на master → getContainerValue на backup (intruder denied) → sleep(3000) → getContainerValue OK на master → OK на backup |
| `testLockExpirationOnBackup` | create → lockObject(2000ms) на backup → getContainerValue на backup → getContainerValue на master (intruder denied) → sleep(3000) → getContainerValue OK на backup → OK на master |

## 3. Константы
```java
private static final int OWNER_CLIENT_ID = 100;
private static final int INTRUDER_CLIENT_ID = 200;
private static final long REPLICATION_DELAY_MS = 100;
```

## 4. Утилиты
```java
private OrderedPayload op(Long order, String val) {
    return OrderedPayload.of(order, val.getBytes(StandardCharsets.UTF_8));
}

private Payload p(String val) {
    return Payload.of(val.getBytes(StandardCharsets.UTF_8));
}

private byte[] bytes(String val) {
    return val.getBytes(StandardCharsets.UTF_8);
}
```

## 5. Файл для модификации
| Файл | Действие |
|------|----------|
| `src/test/java/.../smart/OrderedMapOperationsTest.java` | **Исправить существующие тесты + добавить новые** |

## 6. Верификация
1. **Компиляция:** `mvn test-compile -q` — без ошибок
2. **Запуск тестов:** `mvn test -Dtest=OrderedMapOperationsTest -q` — все тесты проходят

## 7. Примечания
- `createOrderedMap` без `setMode`
- После create — 100ms на репликацию
- `addElementOrderedMap` принимает `List<OrderedPayload>` и `List<Payload>`
- `removeFromContainer` для OrderedMap принимает `bytes(elementKey)`
- `removeElementAtPosition` удаляет по weight range (pos, endPos)
- `streamElementInRangeOrderedMap` принимает `startWeight`, `endWeight`, `reverse`
- Все тесты с блокировками вызывают методы с `clientId`
- TTL expiration использует `Thread.sleep(3000)` при TTL=2000ms
- Lock expiration использует `Thread.sleep(3000)` при lock TTL=2000ms
- `assertDenied()` для PERMISSION_DENIED