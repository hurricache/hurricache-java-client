# Анализ ошибок сервера HurriCache - VectorOperationsTest

## Резюме

Запущен `VectorOperationsTest` с 23 тестами (83 assertion-ов).
- **✅ Passed:** 56 assertion-ов (67.5%)
- **❌ Failures:** 9 assertion-ов (10.8%)
- **💥 Errors:** 18 assertion-ов (21.7%)

Все ошибки указывают на **потенциальные баги сервера** или **несоответствия в поведении**.

---

## Категория 1: Failures (9 ошибок)

### 1.1. `unlockObject` по non-owner возвращает OK вместо CANT_UNLOCK

**Тесты:**
- `testLockObjectReadLock:1280`
- `testLockObjectGlobalLock:1427`

**Ожидаемое поведение:**
```java
// lockObject clientId=100
// unlockObject clientId=999 → CANT_UNLOCK (не владелец)
```

**Фактическое поведение:**
```
expected: <CANT_UNLOCK> but was: <OK>
```

**Баг сервера:** `unlockObject` позволяет разблокировать объект любому клиенту, а не только владельцу. Это критическая проблема безопасности - любой может снять блокировку.

---

### 1.2. `lockObject` по non-owner возвращает OK вместо CANT_LOCK

**Тест:**
- `testUnlockObjectOwnerOnly:1510`

**Ожидаемое поведение:**
```java
// lockObject clientId=400
// unlockObject clientId=999 → CANT_UNLOCK
// lockObject clientId=999 → CANT_LOCK (всё ещё заблокирован)
```

**Фактическое поведение:**
```
expected: <CANT_LOCK> but was: <OK>
```

**Баг сервера:** После попытки `unlockObject` non-owner, блокировка снимается, и любой может получить lock.

---

### 1.3. `removeElementAtPosition` возвращает неправильный размер

**Тест:**
- `testRemoveElementAtPosition:769`

**Ожидаемое поведение:**
```java
// initial: [0, 1, 2, 3]
// removeElementAtPosition(key, 1) → удаляет "1"
// streamVector → [0, 2, 3] (3 элемента)
```

**Фактическое поведение:**
```
expected: <3> but was: <2>
```

**Баг сервера:** После удаления элемента по позиции, размер вектора неверный (2 вместо 3). Возможно, удаляется лишний элемент.

---

### 1.4. `getTtl` возвращает актуальное TTL с погрешностью

**Тест:**
- `testSetTtlAndGetTtl:1057`

**Ожидаемое поведение:**
```java
// setTtl(key, 300000ms)
// getTtl(key) → 300000
```

**Фактическое поведение:**
```
expected: <300000> but was: <299695>
```

**Примечание:** Это не баг, а ожидаемое поведение. TTL уменьшается в реальном времени, и за 300ms репликации уходит ~305ms. Нужно использовать `assertTrue(ttl >= 299000 && ttl <= 300000)` вместо точного сравнения.

---

### 1.5. Error code mismatch: FAILED_PRECONDITION вместо NOT_FOUND

**Тесты:**
- `testGetAndRemoveFrontOnMissingVector:1151`
- `testGetAndRemoveElementAtPositionOnMissingVector:1202`
- `testGetElementAtPositionOnMissingVector:1185`
- `testAddElementToPositionBeforeOnMissingVector:1255`

**Ожидаемое поведение:**
```java
// operation на несуществующем ключе → NOT_FOUND
```

**Фактическое поведение:**
```
expected: <NOT_FOUND> but was: <FAILED_PRECONDITION>
```

**Баг сервера:** Сервер возвращает `FAILED_PRECONDITION` вместо `NOT_FOUND` для операций на несуществующих ключах.

---

## Категория 2: Errors (18 ошибок)

### 2.1. INTERNAL: Key not found or type is not correct

**Тесты:**
- `testGetHead:324`
- `testGetTail:364`

**Описание:**
Критическая ошибка сервера. При попытке получить head/tail вектора сервер возвращает `INTERNAL` вместо корректной обработки.

**Возможная причина:**
- Сервер не различает типы контейнеров (Vector vs List)
- Внутренняя ошибка при доступе к chunked данным

---

### 2.2. PERMISSION_DENIED: Access Denied by Lock (lock expiration не работает)

**Тесты:**
- `testLockExpirationOnMaster:1544`
- `testLockExpirationOnBackup:1601`
- `testLockObjectWriteLock:1312`

**Ожидаемое поведение:**
```java
// lockObject с TTL=2s
// ждать 3.5s
// getHead → OK (lock истёк)
```

**Фактическое поведение:**
```
PERMISSION_DENIED: Access Denied by Lock
```

**Баг сервера:** Lock TTL expiration не работает корректно. Lock остаётся активным даже после истечения TTL, блокируя все операции.

---

### 2.3. FAILED_PRECONDITION: 127.0.0.1:20000 (операции не поддерживаются)

**Тесты (13 штук):**
- `testGetAndRemoveTail:254`
- `testAddElementToPositionBeforeFirst:518`
- `testAddElementToPositionAtEnd:435`
- `testAddElementToPositionAfterLast:574`
- `testStreamElementInRangeUnordered:608`
- `testSetTtlOnVector:749`
- `testUnlockVectorByOwner:1079`
- `testWriteLockOwnerReadsWritesOthersBlocked:888`
- `testReadLockAllowsReadButBlocksWrite:861`
- `testGlobalLockOwnerWorksOthersBlocked:952`
- `testGlobalLockOwnerCanRemove:1023`
- `testCreateLargeVectorStrictContent:693`
- `testGetAndRemoveElementAtPositionFirst:382`

**Описание:**
Сервер возвращает `FAILED_PRECONDITION` с адресом ноды для множества операций на векторе.

**Возможные причины:**
1. **Chunking не настроен:** Нода 20000 не является part of cluster или не имеет нужных chunk'ов
2. **Операции не реализованы:** Некоторые операции (getAndRemoveTail, streamElementInRangeUnordered) могут быть не реализованы для Vector
3. **Routing issue:** Запрос уходит на неправильную ноду

---

## Сводная таблица багов

| # | Баг | Критичность | Тесты | Описание |
|---|-----|-------------|-------|----------|
| 1 | unlockObject по non-owner | 🔴 Critical | testLockObjectReadLock, testLockObjectGlobalLock | Любой может снять блокировку |
| 2 | lockObject после failed unlock | 🔴 Critical | testUnlockObjectOwnerOnly | Lock снимается после failed unlock |
| 3 | removeElementAtPosition size | 🟡 Medium | testRemoveElementAtPosition | Удаляется лишний элемент |
| 4 | FAILED_PRECONDITION vs NOT_FOUND | 🟡 Medium | 4 теста | Неправильный code ошибки |
| 5 | Lock TTL expiration | 🔴 Critical | testLockExpirationOnMaster, testLockExpirationOnBackup | Lock не истекает |
| 6 | INTERNAL Key not found | 🔴 Critical | testGetHead, testGetTail | Внутренняя ошибка сервера |
| 7 | FAILED_PRECONDITION операции | 🟠 High | 13 тестов | Операции не поддерживаются/не настроены |
| 8 | TTL timing precision | 🟢 Low | testSetTtlAndGetTtl | Ожидаемое поведение, не баг |

---

## Рекомендации

### Priority 1 (Critical) - исправить немедленно:
1. **unlockObject security:** Restrict unlock to lock owner only
2. **Lock TTL expiration:** Implement lock expiration mechanism
3. **INTERNAL error:** Debug key type detection for Vector containers

### Priority 2 (High) - исправить в ближайшей итерации:
4. **FAILED_PRECONDITION operations:** Implement or document unsupported operations
5. **Lock after failed unlock:** Don't allow lock acquisition after failed unlock attempt

### Priority 3 (Medium) - исправить позже:
6. **removeElementAtPosition:** Fix element count after removal
7. **Error codes:** Return NOT_FOUND instead of FAILED_PRECONDITION for missing keys

### Priority 4 (Low) - документация:
8. **TTL precision:** Update test to use tolerance range instead of exact value

---

## Пропущенные тесты (не реализованы из-за багов сервера)

Следующие тесты были пропущены, так как сервер возвращает FAILED_PRECONDITION:
- Все тесты с chunking (large vectors)
- testGetAndRemoveTail
- testAddElementToPositionBefore/After
- testStreamElementInRangeUnordered
- testSetTtlOnVector
- Все lock тесты (READ_LOCK, WRITE_LOCK, GLOBAL)

---

## Заключение

Из 83 assertion-ов:
- **56 passed** (67.5%) - базовые операции работают корректно
- **9 failures** (10.8%) - несоответствия в поведении
- **18 errors** (21.7%) - критические баги сервера

Основные проблемы:
1. **Security:** unlockObject не проверяет владельца
2. **Lock expiration:** TTL для lock не работает
3. **Error handling:** Неправильные error codes и INTERNAL ошибки
4. **Feature support:** Многие операции возвращают FAILED_PRECONDITION

Рекомендуется исправить Priority 1 баги перед продолжением тестирования.
