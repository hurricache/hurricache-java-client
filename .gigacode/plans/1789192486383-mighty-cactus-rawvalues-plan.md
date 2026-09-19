# План: RawValuesTest — Скалярные операции + Блокировки

## 1. Анализ текущего покрытия RawValuesTest

### Текущее покрытие: НИЗКОЕ
| Метод | Статус | Примечание |
|-------|--------|------------|
| `createKeyValue` | ✅ | Есть тесты |
| `getValue` | ✅ | Есть тесты |
| `updateKeyValue` | ✅ | Есть тесты |
| `remove` | ✅ | Есть тесты |
| `existKey` | ✅ | Есть тесты |
| `getAndDeleteValue` | ✅ | Есть тесты |
| `setTtl` | ❌ | Нет тестов |
| `getTtl` | ❌ | Нет тестов |
| `lockObject` | ❌ | Нет тестов |
| `unlockObject` | ❌ | Нет тестов |

**Итого:** 6 из 10 методов покрыто, 4 требуют тестов.

---

## 2. Структура тестов (4 секции)

### Секция 1: TTL операции (setTtl, getTtl)
| Тест | Описание |
|------|----------|
| `testSetTtlOnScalar` | Установка TTL на скаляр через `setTtl(key, hint, ttlMs, clientId, timeout)` |
| `testGetTtlAfterSet` | Получение TTL после установки → TTL > 0 |
| `testTtlExpiration` | TTL=100мс → sleep(200) → getValue → NOT_FOUND |

### Секция 2: Блокировки (lock/unlock)
| Тест | Описание |
|------|----------|
| `testReadLockAllowsParallelReads` | READ_LOCK: owner и intruder читают параллельно, write → PERMISSION_DENIED |
| `testReadLockBlocksWrites` | READ_LOCK: intruder пишет → assertDenied |
| `testWriteLockExclusiveAccess` | WRITE_LOCK: owner читает/пишет, intruder не может ни читать, ни писать |
| `testGlobalLockExclusiveAccess` | GLOBAL: только owner работает, все остальные blocked |
| `testIntruderCannotGetWriteLock` | Owner имеет WRITE_LOCK → intruder не получает WRITE_LOCK |
| `testIntruderCannotUnlock` | Intruder не может разблокировать → CANT_UNLOCK |
| `testMultipleReadLocks` | Два READ_LOCK одновременно на один ключ |

### Секция 3: Lock + Scalar Operations
| Тест | Описание |
|------|----------|
| `testLockThenRemove` | WRITE_LOCK → remove → OK → unlock |
| `testLockThenGetAndDeleteValue` | WRITE_LOCK → getAndDeleteValue → OK → unlock |
| `testLockThenExistKey` | WRITE_LOCK → existKey → OK → unlock |

### Секция 4: Lock Expiration
| Тест | Описание |
|------|----------|
| `testWriteLockExpiration` | WRITE_LOCK на 2 сек → sleep(3с) → intruder получает WRITE_LOCK |
| `testReadLockExpiration` | READ_LOCK на 2 сек → sleep(3с) → intruder получает READ_LOCK |

---

## 3. Ключевые конвенции

### Константы:
```java
private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
private static final int OWNER_CLIENT_ID = 100;
private static final int INTRUDER_CLIENT_ID = 200;
```

### Утилиты:
```java
private byte[] bytes(String val) { return val.getBytes(StandardCharsets.UTF_8); }
```

### Справка по блокировкам:
- **READ_LOCK** — я могу читать, другие тоже могут читать параллельно, никто не может писать
- **WRITE_LOCK** — я могу читать и писать, другие не могут ничего (ни читать, ни писать)
- **GLOBAL** — только я делаю любые операции, все остальные блокируются

### Правила:
- В тестах по блокировкам вызывать все методы с `clientId` параметром
- Для блокировок использовать `bytes(key)` вместо `key` (byte[])

### Паттерн assertDenied:
```java
assertDenied(client.someMethod(..., INTRUDER_CLIENT_ID, ...));
```

### Паттерн CANT_UNLOCK:
```java
LockStatus status = client.unlockObject(bytes(key), null, INTRUDER_CLIENT_ID).get();
assertEquals(LockStatus.CANT_UNLOCK, status);
```

---

## 4. Файлы для модификации

| Файл | Действие |
|------|----------|
| `src/test/java/.../RawValuesTest.java` | **Добавить** 12 новых тестов в 4 секции |

---

## 5. Верификация

1. **Компиляция:** `mvn test-compile -q` — без ошибок
2. **Запуск тестов:** `mvn test -Dtest=RawValuesTest -q` — все тесты проходят
3. **Покрытие:** Убедиться что все 4 секции покрыты тестами

---

## 6. Примечания

- Все тесты по блокировкам используют `bytes(key)` для byte[] параметров
- Все методы в lock-тестах вызываются с `clientId`
- TTL expiration использует `Thread.sleep(200)` при TTL=100мс
- Lock expiration использует `Thread.sleep(3000)` при lock TTL=2 сек
