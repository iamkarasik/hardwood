/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import dev.morling.hardwood.internal.reader.ColumnValueIterator;
import dev.morling.hardwood.internal.reader.ColumnarPqRowImpl;
import dev.morling.hardwood.internal.reader.MutableStruct;
import dev.morling.hardwood.internal.reader.PqRowImpl;
import dev.morling.hardwood.internal.reader.RecordAssembler;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Provides row-oriented iteration over a Parquet file.
 * Uses parallel prefetching of column values for efficient reading.
 */
public class RowReader implements Iterable<PqRow>, AutoCloseable {

    private static final int PREFETCH_BATCH_SIZE = 16384;
    private static final ExecutorService EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final FileSchema schema;
    private final FileChannel channel;
    private final List<RowGroup> rowGroups;
    private final boolean flatSchema;
    private boolean closed;

    public RowReader(FileSchema schema, FileChannel channel, List<RowGroup> rowGroups, long totalRows) {
        this.schema = schema;
        this.channel = channel;
        this.rowGroups = rowGroups;
        this.flatSchema = schema.isFlatSchema();
    }

    @Override
    public Iterator<PqRow> iterator() {
        return new PqRowIterator();
    }

    @Override
    public void close() {
        closed = true;
    }

    private List<ColumnValueIterator> createColumnIterators(RowGroup rowGroup) {
        List<ColumnValueIterator> iterators = new ArrayList<>();
        try {
            for (int i = 0; i < schema.getColumnCount(); i++) {
                ColumnReader reader = new ColumnReader(channel, schema.getColumn(i), rowGroup.columns().get(i));
                iterators.add(reader.createIterator());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return iterators;
    }

    private class PqRowIterator implements Iterator<PqRow> {

        private final RecordAssembler assembler = new RecordAssembler(schema);
        private final Iterator<RowGroup> rowGroupIterator = rowGroups.iterator();
        private List<ColumnValueIterator> columnIterators;
        private List<TypedColumnData> prefetchedColumns;
        private int prefetchedRecordCount;
        private int currentRecordIndex;

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (currentRecordIndex < prefetchedRecordCount) {
                return true;
            }
            return prefetchBatch();
        }

        @Override
        public PqRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows available");
            }
            return fetchNextRow();
        }

        private PqRow fetchNextRow() {
            PqRow row;
            if (flatSchema) {
                row = new ColumnarPqRowImpl(prefetchedColumns, currentRecordIndex, schema);
            }
            else {
                MutableStruct rowValues = assembler.assembleRow(prefetchedColumns, currentRecordIndex);
                row = new PqRowImpl(rowValues, schema);
            }
            currentRecordIndex++;
            return row;
        }

        private boolean prefetchBatch() {
            while (true) {
                if (columnIterators != null) {
                    if (prefetchFromCurrentIterators()) {
                        return true;
                    }
                }

                if (!rowGroupIterator.hasNext()) {
                    return false;
                }
                columnIterators = createColumnIterators(rowGroupIterator.next());
            }
        }

        private boolean prefetchFromCurrentIterators() {
            List<Future<TypedColumnData>> futures = new ArrayList<>();

            for (ColumnValueIterator iter : columnIterators) {
                futures.add(EXECUTOR.submit(() -> iter.prefetch(PREFETCH_BATCH_SIZE)));
            }

            try {
                prefetchedColumns = new ArrayList<>();
                for (Future<TypedColumnData> future : futures) {
                    prefetchedColumns.add(future.get());
                }

                prefetchedRecordCount = prefetchedColumns.isEmpty() ? 0 : prefetchedColumns.get(0).recordCount();
                currentRecordIndex = 0;

                if (prefetchedRecordCount == 0) {
                    columnIterators = null;
                    return false;
                }

                return true;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while prefetching", e);
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Failed to prefetch column data", e.getCause());
            }
        }
    }
}
