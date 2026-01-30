/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point for reading Parquet files with a shared thread pool.
 *
 * <p>Use this when reading multiple files to share the executor across readers:</p>
 * <pre>{@code
 * try (Hardwood hardwood = Hardwood.create()) {
 *     ParquetFileReader file1 = hardwood.open(path1);
 *     ParquetFileReader file2 = hardwood.open(path2);
 *     // ...
 * }
 * }</pre>
 *
 * <p>For single-file usage, {@link ParquetFileReader#open(Path)} is simpler.</p>
 */
public class Hardwood implements AutoCloseable {

    private final ExecutorService executor;

    private Hardwood(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Create a new Hardwood instance with a thread pool sized to available processors.
     */
    public static Hardwood create() {
        return create(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Create a new Hardwood instance with a thread pool of the specified size.
     */
    public static Hardwood create(int threads) {
        AtomicInteger threadCounter = new AtomicInteger(0);
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "page-reader-" + threadCounter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        ExecutorService executor = Executors.newFixedThreadPool(threads, threadFactory);
        return new Hardwood(executor);
    }

    /**
     * Open a Parquet file for reading.
     */
    public ParquetFileReader open(Path path) throws IOException {
        return ParquetFileReader.open(path, executor);
    }

    /**
     * Get the executor service used by this instance.
     */
    public ExecutorService executor() {
        return executor;
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
