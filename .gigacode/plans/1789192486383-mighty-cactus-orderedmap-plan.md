# План: Создание OrderedMapOperationsTest

## 1. Анализ текущего покрытия OrderedMap

### Текущее покрытие: ОЧЕНЬ НИЗКОЕ
| Метод | Статус | Примечание |
|-------|--------|------------|
| `createOrderedMap` (empty) | ❌ | Нет тестов |
| `createOrderedMap` (with data) | ❌ | Нет тестов |
| `createOrderedMap` (large, chunking) | ⚠️ Частично | LargeContainerStrictContentTest (только chunking) |
| `streamOrderedMap` (empty) | ❌ | Нет тестов |
| `streamOrderedMap` (with data) | ⚠️ Частично | В large test |
| `addElementOrderedMap` | ❌ | Только cluster test (batch replication) |
| `removeFromContainer(key)` | ❌ | Нет тестов |
| `removeFromContainer(ContainerType)` | ❌ | Нет тестов |
| `containsContainerKey` | ❌ | Нет тестов |
| `removeElementAtPosition` | ❌ | Нет тестов |
| `streamElementInRangeOrderedMap` | ❌ | Нет тестов |
| `getAndRemoveContainerValue` | ❌ | Нет тестов |
| `remove` (container) | ❌ | Нет тестов |
| `getSize` | ❌ | Нет тестов |
| `getContainerValue` | ❌ | Нет тестов |
| `updateContainerValue` | ❌ | Нет тестов |
| `setTtl` / `getTtl` | ❌ | Нет тестов |
| TTL expiration | ❌ | Нет тестов |
| Блокировки (lock/unlock) | ❌ | Нет тестов |
| Lock expiration | ❌ | Нет тестов |
| Unsupported methods | ❌ | Нет тестов |

**Итого:** 0 из 27 методов полностью покрыты, 2 частично.

---

## 2. Создание OrderedMapOperationsTest

### Расположение:
`/ext/hurricache-java-client/src/test/java/com/hurricache/client/standalone/simple/OrderedMapOperationsTest.java`

### Наследование:
```java
public class OrderedMapOperationsTest extends TestBase
```

### Структура тестов (13 секций, ~45 тестов):

#### Секция 1: Создание OrderedMap (createOrderedMap)
| Тест | Описание |
|------|----------|
| `testCreateEmptyOrderedMap` | Создание пустого OrderedMap через `createOrderedMap(key, Map.of())` |
| `testCreateOrderedMapWithInitialData` | Создание с начальными данными (3 элемента с весами) |
| `testCreateOrderedMapWithLargeDataChunking` | Создание с 1500+ элементами для проверки чанкинга |

#### Секция 2: streamOrderedMap
| Тест | Описание |
|------|----------|
| `testStreamOrderedMapEmpty` | Пустой OrderedMap → пустой Map |
| `testStreamOrderedMapWithData` | OrderedMap с данными → возвращает все содержимое |
| `testStreamOrderedMapWithClientId` | streamOrderedMap с явным clientId |

#### Секция 3: addElementOrderedMap (через addElementWithWeight)
| Тест | Описание |
|------|----------|
| `testAddElementOrderedMap` | Добавление элементов, возврат количества |
| `testAddElementOrderedMapDuplicates` | OrderedMap допускает дубликаты (одинаковые key с разными весами) |
| `testAddElementOrderedMapEmptyList` | Пустой список → 0 добавлено |

#### Секция 4: removeFromContainer по ключу
| Тест | Описание |
|------|----------|
| `testRemoveFromContainerByKey` | Удаление существующего элемента → 1+ (дубликаты) |
| `testRemoveFromContainerByKeyNotFound` | Удаление несуществующего → 0 |
| `testRemoveFromContainerFromEmptyMap` | Удаление из пустого контейнера → 0 |

#### Секция 5: removeFromContainer с ContainerType
| Тест | Описание |
|------|----------|
| `testRemoveFromContainerWithType` | Удаление через `removeFromContainer(key, hint, ContainerType.ORDERED_MAP, keys, values)` |
| `testRemoveFromContainerWithTypeNotFound` | Удаление по неверному ключу/значению → 0 |

#### Секция 6: containsContainerKey
| Тест | Описание |
|------|----------|
| `testContainsContainerKeyExists` | Существующий ключ → true |
| `testContainsContainerKeyNotExists` | Несуществующий ключ → false |

#### Секция 7: removeElementAtPosition (weight range)
| Тест | Описание |
|------|----------|
| `testRemoveElementAtPosition` | Удаление элементов в диапазоне весов [20, 40] |
| `testRemoveElementAtPositionSamePos` | Удаление с совпадающими границами (pos = endPos) |

#### Секция 8: streamElementInRangeOrderedMap
| Тест | Описание |
|------|----------|
| `testStreamElementInRangeOrderedMap` | Возврат элементов в диапазоне весов |
| `testStreamElementInRangeOrderedMapReverse` | Reverse=true → обратный порядок |
| `testStreamElementInRangeOrderedMapNoMatch` | Вне диапазона → пустой ответ |

#### Секция 9: getAndRemoveContainerValue
| Тест | Описание |
|------|----------|
| `testGetAndRemoveContainerValue` | Извлечение + удаление → старое значение |
| `testGetAndRemoveContainerValueNotFound` | Несуществующий → NOT_FOUND |

#### Секция 10: remove контейнера
| Тест | Описание |
|------|----------|
| `testRemoveExistingContainer` | Удаление существующего → true |
| `testRemoveNonExistentContainer` | Удаление несуществующего → NOT_FOUND |

#### Секция 11: getSize
| Тест | Описание |
|------|----------|
| `testGetSizeEmptyMap` | Пустой → 0 |
| `testGetSizeWithElements` | С элементами → корректное количество |

#### Секция 12: getContainerValue + updateContainerValue
| Тест | Описание |
|------|----------|
| `testGetContainerValue` | Получение значения по ключу |
| `testGetContainerValueNotFound` | Несуществующий ключ → NOT_FOUND |
| `testUpdateContainerValue` | Обновление → возврат старого значения |
| `testUpdateContainerValueNotFound` | Обновление несуществующего → NOT_FOUND |

#### Секция 13: TTL операции
| Тест | Описание |
|------|----------|
| `testSetTtl` | Установка TTL на контейнер |
| `testGetTtl` | Получение TTL после установки |
| `testTtlExpiration` | После истечения TTL → get вернет NOT_FOUND (sleep 1.5с при TTL=1) |

#### Секция 14: Блокировки (lock/unlock)
| Тест | Описание |
|------|----------|
| `testReadLockParallelReads` | READ_LOCK: несколько клиентов читают параллельно |
| `testWriteLockExclusiveAccess` | WRITE_LOCK: только владелец читает/пишет |
| `testGlobalLockExclusiveAccess` | GLOBAL: только владелец, все остальные blocked |
| `testUnlockByOwnerOnly` | unlockObject: только владелец может разблокировать |
| `testReadLockBlocksWrites` | READ_LOCK: чтение OK, запись → assertDenied |

#### Секция 15: Истечение блокировки
| Тест | Описание |
|------|----------|
| `testReadLockExpiration` | READ_LOCK на 2 сек → sleep 3с → лок снят, другой клиент может читать |
| `testWriteLockExpiration` | WRITE_LOCK на 2 сек → sleep 3с → лок снят, другой клиент получает WRITE_LOCK |
| `testGlobalLockExpiration` | GLOBAL на 2 сек → sleep 3с → лок снят, другой клиент получает WRITE_LOCK |

#### Секция 16: Методы не применимые к OrderedMap
| Тест | Описание |
|------|----------|
| `testUnsupportedMethodsForOrderedMap` | getHead, getTail, getFront, getElementAtPosition, streamElementInRangeUnordered → ошибка |

---

## 3. Ключевые конвенции

### Константы:
```java
private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
private static final int DEFAULT_CLIENT_ID = 101;
private static final int OWNER_CLIENT_ID = 100;
private static final int INTRUDER_CLIENT_ID = 200;
```

### Утилиты:
```java
private byte[] bytes(String val) { return val.getBytes(StandardCharsets.UTF_8); }
private Payload p(String val) { return Payload.of(val.getBytes(StandardCharsets.UTF_8)); }
private OrderedPayload op(Long order, String val) { return OrderedPayload.of(order, val.getBytes(StandardCharsets.UTF_8)); }
private OrderedPayload op(byte[] value, Long order) { return OrderedPayload.of(value, order); }
private String str(Payload payload) { return new String(payload.getValue(), StandardCharsets.UTF_8); }
```

### Паттерн NOT_FOUND:
```java
ExecutionException ex = Assertions.assertThrows(
    ExecutionException.class,
    () -> client.someMethod(...).get()
);
Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
```

### Паттерн assertDenied (перехват PERMISSION_DENIED / Access Denied by Lock):
```java
assertDenied(client.someMethod(..., INTRUDER_CLIENT_ID, ...));
```

### Паттерн CANT_UNLOCK:
```java
LockStatus status = client.unlockObject(key, wrongClientId).get();
assertEquals(LockStatus.CANT_UNLOCK, status);
```

### Паттерн истечения блокировки:
```java
LockStatus lock = client.lockObject(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
assertEquals(LockStatus.OK, lock);
Thread.sleep(3000);
LockStatus newLock = client.lockObject(key, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
assertEquals(LockStatus.OK, newLock);
```

---

## 4. Файлы для модификации

| Файл | Действие |
|------|----------|
| `src/test/java/.../OrderedMapOperationsTest.java` | **Создать** — новый файл со всеми тестами |

---

## 5. Верификация

1. **Компиляция:** `mvn compile test-compile -q` — без ошибок
2. **Запуск тестов:** `mvn test -Dtest=OrderedMapOperationsTest -q` — все тесты проходят
3. **Покрытие:** Убедиться что все 16 секций покрыты тестами

---

## 6. Примечания по реализации

- OrderedMap использует `OrderedPayload` (weight + key) вместо обычных ключей
- `addElementWithWeight` используется для добавления элементов в OrderedMap
- `removeFromContainer` для OrderedMap **может вернуть > 1** если есть дубликаты (одинаковый key с разными весами)
- `removeElementAtPosition` удаляет по weight range (pos, endPos)
- `streamElementInRangeOrderedMap` возвращает элементы в диапазоне весов
- Все тесты с блокировками вызывают методы с `clientId` параметром
- TTL expiration тесты используют `Thread.sleep(1500)` при TTL=1
- Lock expiration тесты используют `Thread.sleep(3000)` при lock TTL=2
- `streamOrderedMap` возвращает `Map<OrderedPayload, Payload>` — проверка через stream/anyMatch
