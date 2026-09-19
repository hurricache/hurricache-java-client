# План: Добавление непокрытых тестов для Atomic Operations

## 1. Анализ текущего покрытия AtomicOperationsTest

### Текущие тесты (7 тестов):
| Тест | Что проверяет |
|------|---------------|
| `testAtomicCreate` | atomicCreate + atomicLoad |
| `testAtomicStore` | atomicCreate + atomicStore + atomicLoad |
| `testAtomicExchange` | atomicCreate + atomicExchange + atomicLoad |
| `testAtomicAdd` | atomicCreate + atomicAdd + atomicLoad |
| `testAtomicSub` | atomicCreate + atomicSub + atomicLoad |
| `testAtomicAnd` | atomicCreate + atomicAnd + atomicLoad |
| `testAtomicOr` | atomicCreate + atomicOr + atomicLoad |
| `testAtomicXor` | atomicCreate + atomicAnd + atomicOr + atomicXor chain |
| `testAtomicCompareAndSet` | CAS success/failure |
| `testAtomicLoadAndDelete` | atomicLoadAndDelete |

### Критические пробелы:

| Метод | Статус | Примечание |
|-------|--------|------------|
| `existKey` | ❌ **НЕ ТЕСТИРУЕТСЯ** | Нет ни одного теста |
| `remove` (успешный) | ⚠️ Частично | Только error path, нет success path |
| `atomicAdd` negative | ❌ Нет | Отрицательный delta (декремент) |
| `atomicAdd` overflow | ❌ Нет | Long.MAX_VALUE |
| `atomicSub` underflow | ❌ Нет | Отрицательный результат |
| `setTtl` на atomic | ❌ Нет | Только на KV key |
| `getTtl` на atomic | ❌ Нет | Только на KV key |
| `lockObject` на atomic | ❌ Нет | Только на KV key |
| Блокировки для write операций | ❌ Нет | WRITE_LOCK блокирует чтение |
| Истечение блокировки | ❌ Нет | Lock TTL expiration |

---

## 2. План добавления тестов

**Итого новых тестов: ~28**

### Секция 1: existKey (полностью непокрыто)
| Тест | Описание |
|------|----------|
| `testExistKeyExisting` | existKey на существующем atomic key → true |
| `testExistKeyNonExistent` | existKey на несуществующем → false |
| `testExistKeyWithClientId` | existKey с явным clientId |

### Секция 2: remove (success path)
| Тест | Описание |
|------|----------|
| `testRemoveExistingAtomic` | atomicCreate → remove → existKey=false |
| `testRemoveNonExistentAtomic` | remove несуществующего → NOT_FOUND |

### Секция 3: TTL на atomic key
| Тест | Описание |
|------|----------|
| `testSetTtlOnAtomic` | atomicCreate → setTtl → getTtl > 0 |
| `testTtlExpirationOnAtomic` | atomicCreate → setTtl(1) → sleep(1500) → atomicLoad → NOT_FOUND |

### Секция 4: atomicAdd edge cases
| Тест | Описание |
|------|----------|
| `testAtomicAddNegative` | atomicAdd с отрицательным delta → декремент |
| `testAtomicAddOverflow` | atomicAdd на Long.MAX_VALUE → переполнение |
| `testAtomicAddWithClientId` | atomicAdd с явным clientId |

### Секция 5: atomicSub edge cases
| Тест | Описание |
|------|----------|
| `testAtomicSubUnderflow` | atomicSub > текущего значения → отрицательный результат |
| `testAtomicSubWithClientId` | atomicSub с явным clientId |

### Секция 6: atomicCompareAndSet edge cases
| Тест | Описание |
|------|----------|
| `testCASWithClientId` | CAS с явным clientId и timeout |
| `testCASFailureReturnsActual` | CAS failure → возвращает фактическое значение |

### Секция 7: Блокировки на atomic key
| Тест | Описание |
|------|----------|
| `testReadLockOnAtomic` | READ_LOCK: atomicLoad параллельно OK |
| `testWriteLockOnAtomic` | WRITE_LOCK: atomicLoad + atomicAdd OK для владельца |
| `testGlobalLockOnAtomic` | GLOBAL: только владелец, все остальные blocked |
| `testUnlockByOwnerOnly` | unlockObject: только владелец может разблокировать |
| `testReadLockBlocksWrites` | READ_LOCK: atomicLoad OK, atomicAdd → PERMISSION_DENIED |

### Секция 8: Истечение блокировки на atomic
| Тест | Описание |
|------|----------|
| `testReadLockExpirationOnAtomic` | READ_LOCK на 2 сек → sleep 3с → другой клиент может читать |
| `testWriteLockExpirationOnAtomic` | WRITE_LOCK на 2 сек → sleep 3с → новый WRITE_LOCK OK |
| `testGlobalLockExpirationOnAtomic` | GLOBAL на 2 сек → sleep 3с → новый WRITE_LOCK OK |

### Секция 9: ALREADY_EXISTS ошибки
| Тест | Описание |
|------|----------|
| `testAtomicCreateDuplicate` | atomicCreate → повторный atomicCreate → ALREADY_EXISTS |
| `testCreateDifferentTypeOnAtomicKey` | atomicCreate → createSet с тем же ключом → ALREADY_EXISTS |
| `testCreateDifferentTypeOnSetKey` | createSet → atomicCreate с тем же ключом → ALREADY_EXISTS |

---

## 3. Файл для модификации

**Файл:** `/ext/hurricache-java-client/src/test/java/com/hurricache/client/standalone/simple/AtomicOperationsTest.java`

**Добавить ~25 новых тестов** в существующий файл (после существующих тестов).

---

### Паттерн ALREADY_EXISTS:
```java
ExecutionException ex = Assertions.assertThrows(
    ExecutionException.class,
    () -> client.atomicCreate(key, 0L).get()
);
Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
assertEquals(Status.Code.ALREADY_EXISTS, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
```

### Паттерн создания Set для теста type mismatch:
```java
// Создаём atomic key
client.atomicCreate(atomicKey, 0L).get();

// Пытаемся создать Set с тем же ключом
ExecutionException ex = Assertions.assertThrows(
    ExecutionException.class,
    () -> client.createSet(setKey, new ArrayList<>()).get()
);
assertEquals(Status.Code.ALREADY_EXISTS, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
```

## 5. Ключевые конвенции

### Константы:
```java
private static final int DEFAULT_CLIENT_ID = 101;
private static final int OWNER_CLIENT_ID = 100;
private static final int INTRUDER_CLIENT_ID = 200;
private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
```

### Паттерн NOT_FOUND:
```java
ExecutionException ex = Assertions.assertThrows(
    ExecutionException.class,
    () -> client.atomicLoad(key, null).get()
);
Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
```

### Паттерн assertDenied (перехват PERMISSION_DENIED / Access Denied by Lock):
```java
assertDenied(client.atomicLoad(key, null, INTRUDER_CLIENT_ID, TEST_TIMEOUT));
```

### Паттерн истечения блокировки:
```java
LockStatus lock = client.lockObject(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
assertEquals(LockStatus.OK, lock);
Thread.sleep(3000); // истечение
LockStatus newLock = client.lockObject(key, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
assertEquals(LockStatus.OK, newLock);
```

---

## 5. Верификация

1. **Компиляция:** `mvn test-compile -q` — без ошибок
2. **Запуск:** `mvn test -Dtest=AtomicOperationsTest -q` — все тесты проходят
3. **Покрытие:** Все 18 методов интерфейса HurriCacheClientAtomics покрыты

---

## 6. Примечания

- `atomicLoad`, `atomicLoadAndDelete`, `atomicOr` — **read операции** (могут выполняться параллельно под READ_LOCK)
- Все остальные atomic методы — **write операции** (блокируются под READ_LOCK, требуют WRITE_LOCK/GLOBAL)
- `atomicLoadAndDelete` и `atomicCreate` — write операции (удаляют/создают ключ)
- `atomicCompareAndSet` возвращает `AtomicCASResponse` с `getResult()` и `getExpected()`
- TTL expiration тесты используют `Thread.sleep(1500)` при TTL=1
- Lock expiration тесты используют `Thread.sleep(3000)` при lock TTL=2