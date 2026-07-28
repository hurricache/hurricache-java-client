package com.hurricache.client.cluster.stress;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class VectorIndexPerfTest {

    private static final int MESSAGE_SIZE = 100; // байт
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    // Настройки параллелизма для чтения
    private static final int READER_THREADS = 16;
    private static final int DURATION_PER_STAGE_SECONDS = 5;
    private static final int MAX_IN_FLIGHT_PER_THREAD = 100;

    // НАСТРОЙКА РАЗМЕРОВ: дефолтные значения, если ничего не передано снаружи
    private static final int[] DEFAULT_SIZES = {16, 64, 512, 4096, 32768, 262144};

    private static FastCacheAsyncSmartClient client;

    @BeforeAll
    public static void setup() throws Exception {
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, TIMEOUT) {
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(15);
            }
        };
    }

    @AfterAll
    public static void teardown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
    }

    /**
     * Парсит размеры векторов из системного свойства "vector.sizes".
     * Если свойство не задано, возвращает дефолтный набор.
     */
    private int[] parseTargetSizes() {
        String sizesProp = System.getProperty("vector.sizes");
        if (sizesProp == null || sizesProp.trim().isEmpty()) {
            return DEFAULT_SIZES;
        }
        try {
            return Arrays.stream(sizesProp.split(","))
                    .map(String::trim)
                    .mapToInt(Integer::parseInt)
                    .toArray();
        } catch (NumberFormatException e) {
            System.err.println("[WARN] Invalid format for vector.sizes property. Falling back to defaults.");
            return DEFAULT_SIZES;
        }
    }

    @Test
    void benchmarkVectorAccessAtDifferentSizes() throws Exception {
        // Получаем динамически настроенный массив размеров векторов
        int[] targetSizes = parseTargetSizes();

        System.out.println("===============================================================");
        System.out.println(" STARTING VECTOR INDEX ACCESS BENCHMARK ");
        System.out.println(" Target sizes to test: " + Arrays.toString(targetSizes));
        System.out.println(" Threads: " + READER_THREADS + " | In-flight window per thread: " + MAX_IN_FLIGHT_PER_THREAD);
        System.out.println("===============================================================");

        for (int size : targetSizes) {
            runBenchmarkForSize(size);
        }
    }

    private void runBenchmarkForSize(int vectorSize) throws Exception {
        String vectorKeyStr = "perf-vector-" + vectorSize + "-" + System.currentTimeMillis();
        byte[] vectorKey = vectorKeyStr.getBytes(StandardCharsets.UTF_8);

        // 1. Подготовка данных нужного размера с использованием Payload
        List<Payload> initialData = new ArrayList<>(vectorSize);
        for (int i = 0; i < vectorSize; i++) {
            initialData.add(generate100BytePayload("val-" + i));
        }

        // 2. Создание вектора на сервере
        KeyHintData vectorHint = client.createVector(
                vectorKey,
                initialData,
                Duration.ofMinutes(15),
                0,
                TIMEOUT
        ).get();

        assertNotNull(vectorHint, "Vector must be created successfully for size: " + vectorSize);

        // 3. Запуск потоков чтения
        ExecutorService pool = Executors.newFixedThreadPool(READER_THREADS);
        LongAdder readOps = new LongAdder();
        LongAdder readErrors = new LongAdder();

        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < READER_THREADS; i++) {
            AtomicBoolean finalRunning = running;
            pool.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    Semaphore inFlightWindow = new Semaphore(MAX_IN_FLIGHT_PER_THREAD);

                    while (finalRunning.get()) {
                        inFlightWindow.acquire();

                        // Случайный индекс в рамках текущего размера вектора
                        int randomIdx = random.nextInt(vectorSize);

                        client.setMode(Mode.LB_SMART)
                                .getElementAtPosition(vectorKey, vectorHint, randomIdx, 0, TIMEOUT)
                                .whenComplete((res, ex) -> {
                                    inFlightWindow.release();
                                    if (ex == null && res != null && res.getValue() != null) {
                                        readOps.increment();
                                    } else {
                                        readErrors.increment();
                                    }
                                });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Прогрев 500 мс
        startLatch.countDown();
        Thread.sleep(500);

        readOps.reset();
        readErrors.reset();
        long startTime = System.nanoTime();

        // Замер
        Thread.sleep(DURATION_PER_STAGE_SECONDS * 1000L);
        running.getAndSet(false);

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        long elapsedTimeNs = System.nanoTime() - startTime;
        double elapsedTimeSec = elapsedTimeNs / 1_000_000_000.0;
        long totalReads = readOps.sum();
        double tps = totalReads / elapsedTimeSec;

        System.out.printf("Vector Size: %-7d | Elements | Read TPS: %-10.2f | Errors: %d%n",
                          vectorSize, tps, readErrors.sum());
    }

    private Payload generate100BytePayload(String prefix) {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[MESSAGE_SIZE];
        System.arraycopy(prefixBytes, 0, result, 0, Math.min(prefixBytes.length, MESSAGE_SIZE));
        for (int i = prefixBytes.length; i < MESSAGE_SIZE; i++) {
            result[i] = 'x';
        }
        return Payload.of(result);
    }
}