# 📋 Стратегия переименования методов HurriCache Java Client

## 🎯 Цель
Создать **единый, предсказуемый API** где:
- Имя метода = **действие**, а не тип контейнера
- Префикс `get` = чтение (без мутаций) — **ЕДИНЫЙ для всех контейнеров**
- Префикс `add`/`put` = добавление
- Префикс `remove`/`delete` = удаление
- Префикс `update` = изменение существующего
- Префикс `getAnd*` = атомарная операция "прочитай+измени"
- Суффикс `InContainer` = операция ВНУТРИ контейнера (по elementKey)
- Без суффикса = операция НАД контейнером (сам контейнер)

---

## 🔄 КАРТА ПЕРЕИМЕНОВАНИЙ

### 1. УДАЛЕНИЕ — КРИТИЧЕСКИЙ КОНФЛИКТ

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `remove(key)` | **`deleteContainer(key)`** | InterfaceCommon | Удаляет ВЕСЬ контейнер |
| `remove(key, elemKey)` | **`removeElementFromContainer(key, elemKey)`** | MapBased | Удаляет ЭЛЕМЕНТ из контейнера |
| `removeFromContainerByKey(payload)` | **`removeElementFromContainer(key, payload)`** | MapBased | Дубликат, объединить с выше |
| `removeFromContainerByKeyAndValue(keys, values)` | **`removeElementsByValues(key, values)`** | MapBased | Удаляет по значениям |
| `deleteValueInContainer(key, elemKey)` | **`removeElementFromContainer(key, elemKey)`** | (SmartClient) | То же, что removeElement |
| `deleteElementAtPosition(key, pos)` | **`removeElementAtPosition(key, pos)`** | Vector/OrderedSet | Удалить по индексу |

**Правило:** `delete` = удалить объект целиком, `remove` = удалить элемент из коллекции

---

### 2. ЧТЕНИЕ — УНИФИКАЦИЯ stream* → get*

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `getValue(key)` | **`get(key)`** | Scalar | Простое чтение scalar |
| `getMap(key)` | **`getMap(key)`** ✅ | HashMap | Оставить как есть |
| `getSet(key)` | **`getSet(key)`** ✅ | Set | Оставить как есть |
| `getList(key)` | **`getList(key)`** ✅ | List | Оставить как есть |
| `getVector(key)` | **`getVector(key)`** ✅ | Vector | Оставить как есть |
| `getOrderedMap(key)` | **`getOrderedMap(key)`** ✅ | SortedMap | Оставить как есть |
| `getOrderedSet(key)` | **`getOrderedSet(key)`** ✅ | SortedSet | Оставить как есть |
| `getQueue(key)` | **`getQueue(key)`** ✅ | Queue | Оставить как есть |
| `getInContainer(key, elemKey)` | **`getFromContainer(key, elemKey)`** | MapBased | Получить значение по ключу |
| `getValueInContainerByKey(key, elemKey)` | **`getFromContainer(key, elemKey)`** | SmartClient | То же, что getFromContainer |
| `streamMap(key)` | **`getMap(key)`** | SmartClient | **Унифицировать с getMap** |
| `streamSet(key)` | **`getSet(key)`** | SmartClient | **Унифицировать с getSet** |
| `streamList(key)` | **`getList(key)`** | SmartClient | **Унифицировать с getList** |
| `streamOrderedMap(key)` | **`getOrderedMap(key)`** | SmartClient | **Унифицировать с getOrderedMap** |
| `streamOrderedSet(key)` | **`getOrderedSet(key)`** | SmartClient | **Унифицировать с getOrderedSet** |

**Правило:** `get*` = получить ВСЁ контейнер (единый для Standalone и SmartClient)

---

### 3. ПРОВЕРКА НАЛИЧИЯ — ДВА РАЗНЫХ МЕТОДА

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `containsKey(key, elemKey)` | **`containsKey(key, elemKey)`** ✅ | MapBased | Проверка ключа |
| `existInContainerByKey(key, elemBytes)` | **`containsElement(key, elemBytes)`** | SmartClient | Проверка элемента |
| `existInContainerByKey(key, value)` | **`containsValue(key, value)`** | SmartClient | Проверка значения |
| `existInContainerByKey(key, hint, bytes)` | **`containsKey(key, bytes)`** | SmartClient | Дубликат containsKey |

**Правило:** `containsKey` = по ключу, `containsElement` = по элементу, `containsValue` = по значению

---

### 4. ДОБАВЛЕНИЕ — ОТНОСИТЕЛЬНО ЕДИНО

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `addElementUnordered(key, data)` | **`addElements(key, data)`** | Set/MapBased | Убрать "unordered" — понятно из типа |
| `addElementToTail(key, data)` | **`addElementsToTail(key, data)`** ✅ | List/Queue | Оставить |
| `addElementToHead(key, data)` | **`addElementsToHead(key, data)`** ✅ | List | Оставить |
| `addElementAtPosition(key, pos, data)` | **`addElementsAtPosition(key, pos, data)`** ✅ | Vector | Оставить |
| `addElementWithWeight(key, data)` | **`addElementsWithWeight(key, data)`** | OrderedSet | Добавить "s" для консистентности |
| `addElementOrderedMap(key, keys, values)` | **`addElementsToOrderedMap(key, keys, values)`** | SortedMap | Добавить "s" и "To" |
| `putAll(key, keys, values)` | **`putAll(key, keys, values)`** ✅ | MapBased | Оставить (стандарт Java) |
| `addElementOrderedMap(key, keys, values)` | **`addElementsToOrderedMap(key, keys, values)`** | SortedMap | Унифицировать с addElement* |

**Правило:** `addElement*` → `addElements*` (множественное число, т.к. принимает List)

---

### 5. ОБНОВЛЕНИЕ — ДВА РАЗНЫХ МЕТОДА

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `update(key, elemKey, value)` | **`updateInContainer(key, elemKey, value)`** | MapBased | Добавить контекст |
| `updateContainerValue(key, elemKey, value)` | **`updateInContainer(key, elemKey, value)`** | SmartClient | Унифицировать с выше |
| `getAndDeleteValueInContainer(key, elemKey)` | **`getAndRemoveFromContainer(key, elemKey)`** | SmartClient | Более понятное имя |
| `popInContainer(key, elemKey)` | **`popFromContainer(key, elemKey)`** | MapBased | "pop" = getAndRemove |

---

### 6. АТОМАРНЫЕ ОПЕРАЦИИ — ОТНОСИТЕЛЬНО ЕДИНО

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `exchange(key, value)` | **`atomicExchange(key, value)`** | Atomics | Добавить префикс для ясности |
| `add(key, delta)` | **`atomicAdd(key, delta)`** | Atomics | Добавить префикс |
| `atomicSub(key, delta)` | **`atomicSub(key, delta)`** ✅ | Atomics | Оставить |
| `and(key, mask)` | **`atomicAnd(key, mask)`** | Atomics | Добавить префикс |
| `or(key, mask)` | **`atomicOr(key, mask)`** | Atomics | Добавить префикс |
| `xor(key, mask)` | **`atomicXor(key, mask)`** | Atomics | Добавить префикс |
| `compareAndSet(key, expected, new)` | **`atomicCompareAndSet(key, expected, new)`** | Atomics | Добавить префикс |

---

### 7. СПЕЦИАЛЬНЫЕ ОПЕРАЦИИ

| ТЕКУЩЕЕ ИМЯ | НОВОЕ ИМЯ | ГДЕ | ПОЧЕМУ |
|-------------|-----------|-----|--------|
| `getElementInRangeOrderedSet(...)` | **`streamRange(key, start, end, reverse)`** | SortedSet | Упростить |
| `streamElementInRangeOrderedMap(...)` | **`streamRange(key, start, end, reverse)`** | SortedMap | Унифицировать |
| `getElementAtPosition(key, pos)` | **`getElementAt(key, pos)`** | Vector/OrderedSet | Упростить |
| `getHead(key)` | **`getHead(key)`** ✅ | List | Оставить |
| `getTail(key)` | **`getTail(key)`** ✅ | List/Queue | Оставить |
| `getAndDeleteHead(key)` | **`getAndDeleteHead(key)`** ✅ | Queue | Оставить |
| `getAndDeleteTail(key)` | **`getAndDeleteTail(key)`** ✅ | Queue | Оставить |

---

## 📐 ИТОГОВАЯ КОНВЕНЦИЯ

### Префиксы действий
| Префикс | Значение | Примеры |
|---------|----------|---------|
| `create*` | Создать контейнер | `createMap()`, `createList()`, `createSet()` |
| `get` | Прочитать контейнер | `get()`, `getMap()`, `getSet()`, `getList()`, `getFromContainer()` |
| `streamRange` | Прочитать диапазон по весу | `streamRange(start, end, reverse)` |
| `addElements*` | Добавить элементы | `addElements()`, `addElementsToTail()`, `addElementsWithWeight()` |
| `putAll` | Добавить все (Map) | `putAll()` |
| `remove*` | Удалить элемент | `removeElementFromContainer()`, `removeElementAtPosition()` |
| `delete*` | Удалить объект | `deleteContainer()` |
| `updateInContainer` | Обновить элемент | `updateInContainer(key, elemKey, value)` |
| `getAndRemove*` | Атомарное чтение+удаление | `getAndRemoveFromContainer()`, `getAndDeleteHead()` |
| `atomic*` | Атомарная операция | `atomicExchange()`, `atomicAdd()`, `atomicCompareAndSet()` |
| `contains*` | Проверка наличия | `containsKey()`, `containsElement()`, `containsValue()` |
| `getSize` | Получить размер | `getSize()` |
| `getHead`/`getTail` | Концы очереди | `getHead()`, `getTail()` |

### Суффиксы контекста
| Суффикс | Значение | Пример |
|---------|----------|--------|
| *(без суффикса)* | Над контейнером целиком | `deleteContainer()`, `getMap()` |
| `InContainer` | Внутри контейнера | `updateInContainer(key, elemKey, value)` |
| `FromContainer` | Извлечь из контейнера | `getFromContainer(key, elemKey)` |
| `AtPosition` | По индексу | `removeElementAtPosition(key, pos)` |
| `ToTail`/`ToHead` | К концу/началу | `addElementsToTail()` |
| `WithWeight` | С весом/порядком | `addElementsWithWeight()` |
| `Range` | Диапазон по весу | `streamRange(start, end)` |

---

## 🚀 ПРИОРИТЕТЫ РЕАЛИЗАЦИИ

### 🔴 КРИТИЧНО (конфликты)
1. Разделить `deleteContainer()` vs `removeElementFromContainer()`
2. Унифицировать `getFromContainer()` / `updateInContainer()`
3. Унифицировать `containsKey()` / `containsElement()`
4. **Унифицировать `stream*` → `get*` в SmartClient**

### 🟡 ВАЖНО (консистентность)
5. Переименовать `addElements*` (множественное число)
6. Добавить `atomic*` префикс к атомарным операциям
7. Унифицировать `streamRange()` для OrderedMap/Set

### 🟢 ЖЕЛАТЕЛЬНО (удобство)
8. Упростить `getElementAtPosition()` → `getElementAt()`
9. Добавить дефолтные методы-обёртки для удобства

---

## 📝 ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ

### До:
```java
// Standalone/Simple (get*)
client.getMap(key);
client.setMap(key, hint);

// SmartClient (stream*) — РАЗНЫЕ ИМЕНА!
client.streamMap(key);
client.streamSet(key);
client.streamList(key);

// Разные имена для одного действия
client.addElementUnordered(key, hint, List.of(v("a")));  // Set
client.putAll(key, hint, keys, values);                   // Map
client.addElementOrderedMap(key, hint, keys, values);     // OrderedMap
client.addElementWithWeight(key, hint, List.of(...));     // OrderedSet

// Конфликтующие remove
client.remove(key);              // удалить контейнер
client.remove(key, hint, elem);  // удалить элемент
client.removeFromContainerByKey(key, hint, payload);  // удалить элемент
client.deleteValueInContainer(key, hint, bytes);       // удалить элемент
```

### После:
```java
// ЕДИНЫЕ имена для всех клиентов (Standalone + SmartClient)
client.getMap(key);
client.getSet(key);
client.getList(key);
client.getOrderedMap(key);
client.getOrderedSet(key);

// Единый паттерн addElements
client.addElements(key, null, List.of(v("a")));                    // Set
client.putAll(key, null, keys, values);                             // Map (Java стандарт)
client.addElementsToOrderedMap(key, null, keys, values);            // OrderedMap
client.addElementsWithWeight(key, null, List.of(...));              // OrderedSet

// Чёткое разделение remove/delete
client.deleteContainer(key);                                      // удалить контейнер
client.removeElementFromContainer(key, null, elem);               // удалить элемент
client.removeElementFromContainerByValue(key, null, List.of(v("a"))); // удалить по значению
```