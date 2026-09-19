# HurriCache — Матрица поддержки контейнеров

На основе: `cache.proto`

| # | FastCacheAsyncSimpleClient | gRPC (cache.proto) | Описание | Скаляр | Очередь | Лист | Вектор | HashedSet | OrderedSet | HashedMap | OrderedMap | Атомик |
|---|---------------------------|---------------------|----------|:------:|:-------:|:----:|:------:|:---------:|:----------:|:---------:|:----------:|:------:|
| 1 | `setTtl` | `setTtl(TtlRequest)` | Установка/обновление TTL | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 2 | `getTtl` | `getTtl(GetRequest)` | Получение оставшегося TTL | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 3 | `getAndDeleteValue` | `getAndDeleteValue(GetRequest)` | Атомарное чтение с удалением | ✔ | | | | | | | | |
| 4 | `createKeyValue` | `createKeyValue(CreateRequest)` | Создание скалярного элемента | ✔ | | | | | | | | |
| 5 | `getValue` | `getValue(GetRequest)` | Получение скалярного значения | ✔ | | | | | | | | |
| 6 | `updateKeyValue` | `updateValue(UpdateRequest)` | Обновление скалярного значения | ✔ | | | | | | | | |
| 7 | `existKey` | `existKey(GetRequest)` | Проверка существования ключа | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 8 | `remove` | `remove(GetRequest)` | Полное удаление ключа/контейнера | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 9 | `createQueue/List/Vector/Set/OrderedSet/Map/OrderedMap` | `createContainer(CreateContainerRequest)` | Инициализация контейнера | | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | |
| 10 | `getSize` | `getSize(GetRequest)` | Количество элементов в контейнере | | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | |
| 11 | `getAndRemoveFront` | `getAndRemoveFront(GetRequest)` | Извлечение и удаление первого элемента | | ✔ | ✔ | | | | | | |
| 12 | `getHead` | `getHead(GetRequest)` | Получение первого элемента | | ✔ | ✔ | | | | | | |
| 13 | `getTail` | `getTail(GetRequest)` | Получение последнего элемента | | | ✔ | ✔ | | | | | |
| 14 | `getElementAtPosition` | `getElementAtPosition(KeyPositionRequest)` | Чтение элемента по индексу/весу | | | ✔ | ✔ | | ✔ | | | |
| 15 | `streamList/Vector/Set/Map/OrderedSet/OrderedMap` | `getContainer(GetRequest) → stream BatchValueResponse` | Потоковый экспорт элементов | | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | |
| 16 | `streamElementInRangeUnordered/OrderedSet` | `getElementInRange(KeyPositionRequest) → stream BatchValueResponse` | Потоковое чтение диапазона | | | ✔ | ✔ | | ✔ | | ✔ | |
| 17 | `addElementUnordered/Ordered/WithWeight` | `addElement(AddToRequest)` | Добавление элементов в контейнеры | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | |
| 18 | `addElementToTail` | `addElementToTail(AddToRequest)` | Добавление в конец (Push Back) | | ✔ | ✔ | | | | | | |
| 19 | `addElementToHead` | `addElementToHead(AddToRequest)` | Добавление в начало (Push Front) | | | ✔ | ✔ | | | | | |
| 20 | `addElementToPosition` | `addElement(AddToRequest)` | Добавление по указанной позиции | | | ✔ | ✔ | | | | | |
| 21 | `addElementToPositionBefore/After` | `addElementToPositionByValue(AddToValRequest)` | Вставка относительно другого значения | | | ✔ | ✔ | | | | | |
| 22 | `getAndRemoveTail` | `getAndRemoveTail(GetRequest)` | Извлечение и удаление последнего элемента | | | ✔ | ✔ | | | | | |
| 23 | `getAndRemoveElementAtPosition` | `getAndRemoveElementAtPosition(KeyPositionRequest)` | Извлечение по индексу/весу | | | ✔ | ✔ | | ✔ | | | |
| 24 | `removeTail` | `removeTail(GetRequest)` | Удаление последнего элемента | | | ✔ | ✔ | | | | | |
| 25 | `removeHead` | `removeHead(GetRequest)` | Удаление первого элемента | | ✔ | ✔ | | | | | | |
| 26 | `removeElementAtPosition` | `removeElementAtPosition(KeyPositionRequest)` | Удаление по индексу/весу | | | ✔ | ✔ | | ✔ | | | |
| 27 | `removeFromContainer` | `removeFromContainerByKeyValue(RemoveFromContainerRequest)` | Удаление по значению | | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | |
| 28 | `lockObject` | `lockObject(LockRequest)` | Захват блокировки | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 29 | `unlockObject` | `unlockObject(UnLockRequest)` | Освобождение блокировки | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 30 | `atomicLoad` | `atomicLoad(GetRequest)` | Атомарное чтение | | | | | | | | | ✔ |
| 31 | `atomicLoadAndDelete` | `atomicLoadAndDelete(GetRequest)` | Чтение и очистка атомарного значения | | | | | | | | | ✔ |
| 32 | `atomicCreate` | `atomicCreate(AtomicCreate)` | Инициализация атомарного объекта | | | | | | | | | ✔ |
| 33 | `atomicStore` | `atomicStore(AtomicCreate)` | Запись атомарного значения | | | | | | | | | ✔ |
| 34 | `atomicExchange` | `atomicExchange(AtomicCreate)` | Атомарный обмен (Swap) | | | | | | | | | ✔ |
| 35 | `atomicAdd` | `atomicAdd(AtomicCreate)` | Инкремент | | | | | | | | | ✔ |
| 36 | `atomicSub` | `atomicSub(AtomicCreate)` | Декремент | | | | | | | | | ✔ |
| 37 | `atomicOr` | `atomicOr(AtomicCreate)` | Побитовая OR | | | | | | | | | ✔ |
| 38 | `atomicAnd` | `atomicAnd(AtomicCreate)` | Побитовая AND | | | | | | | | | ✔ |
| 39 | `atomicXor` | `atomicXor(AtomicCreate)` | Побитовая XOR | | | | | | | | | ✔ |
| 40 | `atomicCompareAndSet` | `atomicCompareAndSet(AtomicCas)` | Compare-And-Swap | | | | | | | | | ✔ |
| 41 | `getContainerValue` | `getValueInContainer(ContainerGetRequest)` | Получение элемента из контейнера | | | | | | | ✔ | ✔ | |
| 42 | `getAndRemoveContainerValue` | `getAndDeleteValueInContainer(ContainerGetRequest)` | Извлечение и удаление из контейнера | | | | | | | ✔ | ✔ | |
| 43 | `containsContainerKey` | `existKeyInContainer(ContainerGetRequest)` | Проверка наличия ключа в контейнере | | | | | ✔ | ✔ | ✔ | ✔ | |
| 44 | `updateContainerValue` | `updateValueInContainer(UpdateContainerRequest)` | Обновление элемента контейнера | | | | | | | ✔ | ✔ | |
| 45 | `removeFromContainer` | `removeInContainer(ContainerGetRequest)` | Удаление из контейнера по ключу | | | | | ✔ | ✔ | ✔ | ✔ | |
| 46 | `addElementHashMap/OrderedMap` | `addElement(AddToRequest)` | Добавление элементов в карты | | | | | | | ✔ | ✔ | |

## Сводка по контейнерам

| Контейнер | Поддерживаемые операции |
|-----------|------------------------|
| **Скаляр** | `createKeyValue`, `getValue`, `updateKeyValue`, `getAndDeleteValue`, `existKey`, `remove`, `setTtl`, `getTtl`, `lockObject`, `unlockObject`, `addElement` |
| **Очередь (QUEUE)** | `createQueue`, `getSize`, `getAndRemoveFront`, `getHead`, `addElementToTail`, `removeHead`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **Лист (LIST)** | `createList`, `getSize`, `getAndRemoveFront`, `getHead`, `getTail`, `addElementToTail`, `addElementToHead`, `addElementToPosition`, `addElementToPositionBefore/After`, `getAndRemoveTail`, `removeTail`, `removeHead`, `streamList`, `streamElementInRangeUnordered`, `addElement`, `getAndRemoveElementAtPosition`, `removeElementAtPosition`, `removeFromContainer`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **Вектор (VECTOR)** | `createVector`, `getSize`, `getTail`, `getElementAtPosition`, `addElementToHead`, `addElementToPosition`, `addElementToPositionBefore/After`, `getAndRemoveTail`, `getAndRemoveElementAtPosition`, `removeTail`, `streamVector`, `streamSet`, `streamElementInRangeUnordered`, `addElement`, `removeElementAtPosition`, `removeFromContainer`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **HashedSet** | `createSet`, `getSize`, `streamSet`, `addElement`, `removeFromContainer`, `containsContainerKey`, `removeInContainer`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **OrderedSet** | `createOrderedSet`, `getSize`, `getElementAtPosition`, `streamOrderedSet`, `streamElementInRangeOrderedSet`, `addElement`, `getAndRemoveElementAtPosition`, `removeElementAtPosition`, `removeFromContainer`, `containsContainerKey`, `removeInContainer`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **HashedMap** | `createMap`, `getSize`, `streamMap`, `addElementHashMap`, `getContainerValue`, `getAndRemoveContainerValue`, `containsContainerKey`, `updateContainerValue`, `removeInContainer`, `removeFromContainer`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **OrderedMap** | `createOrderedMap`, `getSize`, `streamOrderedMap`, `addElementOrderedMap`, `getContainerValue`, `getAndRemoveContainerValue`, `containsContainerKey`, `updateContainerValue`, `removeInContainer`, `removeFromContainer`, `streamElementInRangeOrderedSet`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |
| **Атомик** | `atomicLoad`, `atomicLoadAndDelete`, `atomicCreate`, `atomicStore`, `atomicExchange`, `atomicAdd`, `atomicSub`, `atomicOr`, `atomicAnd`, `atomicXor`, `atomicCompareAndSet`, `setTtl`, `getTtl`, `existKey`, `remove`, `lockObject`, `unlockObject` |