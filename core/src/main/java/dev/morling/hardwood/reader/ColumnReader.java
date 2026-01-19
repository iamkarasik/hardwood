/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.internal.reader.ColumnBatch;
import dev.morling.hardwood.internal.reader.PageReader;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reader for a column chunk.
 */
public class ColumnReader {

    private final ColumnSchema column;
    private final ColumnMetaData columnMetaData;
    private final PageReader pageReader;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;
    private final long totalValues;

    // State for incremental reading
    private PageReader.Page currentPage;
    private int currentPagePosition;
    private long valuesRead;

    // State for typed batch reading
    private PageReader.TypedPage currentTypedPage;
    private int currentTypedPagePosition;

    // Lookahead buffer (single value)
    private ValueWithLevels lookahead;

    public ColumnReader(FileChannel channel, ColumnSchema column, ColumnChunk columnChunk) throws IOException {
        this.column = column;
        this.columnMetaData = columnChunk.metaData();
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.totalValues = columnMetaData.numValues();

        // Determine the start of the column chunk (dictionary page or data page)
        Long dictOffset = columnMetaData.dictionaryPageOffset();
        long chunkStartOffset = (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : columnMetaData.dataPageOffset();

        // Memory-map the column chunk region
        long chunkSize = columnMetaData.totalCompressedSize();
        MappedByteBuffer mappedChunk = channel.map(FileChannel.MapMode.READ_ONLY, chunkStartOffset, chunkSize);

        this.pageReader = new PageReader(mappedChunk, columnMetaData, column);
    }

    /**
     * Read all values from this column chunk.
     * Null values are represented as null in the returned list.
     */
    public List<Object> readAll() throws IOException {
        List<Object> allValues = new ArrayList<>();

        while (hasNext()) {
            allValues.add(readNext());
        }

        return allValues;
    }

    /**
     * Check if there are more values to read from this column.
     */
    public boolean hasNext() {
        return lookahead != null || valuesRead < totalValues;
    }

    /**
     * Read the next value from this column.
     * For columns with repetition levels (lists), returns a List containing all elements.
     * Returns null for optional fields when the value is not present.
     */
    public Object readNext() throws IOException {
        if (!hasNext()) {
            throw new IllegalStateException("No more values to read");
        }

        // For columns with repetition levels, assemble a list
        if (maxRepetitionLevel > 0) {
            return readList();
        }

        // Simple case - no repetition levels
        return readSingleValue();
    }

    /**
     * Read a single value (no list assembly needed).
     */
    private Object readSingleValue() throws IOException {
        ensurePageLoaded();

        Object value;
        if (maxDefinitionLevel == 0) {
            // Required field - all values present
            value = currentPage.values()[currentPagePosition];
        }
        else {
            // Optional field - check definition level
            if (currentPage.definitionLevels()[currentPagePosition] == maxDefinitionLevel) {
                value = currentPage.values()[currentPagePosition];
            }
            else {
                value = null;
            }
        }

        currentPagePosition++;
        valuesRead++;
        return value;
    }

    /**
     * Read a list by consuming values until the next rep=0.
     * Uses a stack-based algorithm that works for any nesting depth.
     *
     * For list<list<list<T>>> with maxRep=3:
     *   rep=0: new record (start fresh)
     *   rep=1: new list at depth 1
     *   rep=2: new list at depth 2
     *   rep=3: continue list at depth 2 (add element)
     *
     * The repetition level tells us "at which level did we start a new list?"
     * Lower rep = deeper restart (closer to root).
     */
    @SuppressWarnings("unchecked")
    private List<?> readList() throws IOException {
        ValueWithLevels first = nextValue();
        if (first == null || first.defLevel == 0) {
            return null;
        }

        // Stack of lists at each nesting level (index 0 = outermost)
        int depth = maxRepetitionLevel;
        List<Object>[] stack = new List[depth];
        stack[0] = new ArrayList<>();

        // Empty list (def=1 means outermost list exists but has no elements)
        if (first.defLevel == 1) {
            return stack[0];
        }

        // Initialize nested lists for the first value
        for (int i = 1; i < depth; i++) {
            stack[i] = new ArrayList<>();
            stack[i - 1].add(stack[i]);
        }

        // Add first value if present
        if (first.defLevel == maxDefinitionLevel) {
            stack[depth - 1].add(convertValue(first.value));
        }

        // Continue reading while rep > 0 (same record)
        while (peekRepLevel() > 0) {
            ValueWithLevels v = nextValue();

            // Create new lists from v.repLevel to the deepest level
            for (int i = v.repLevel; i < depth; i++) {
                stack[i] = new ArrayList<>();
                stack[i - 1].add(stack[i]);
            }

            // Add value if present
            if (v.defLevel == maxDefinitionLevel) {
                stack[depth - 1].add(convertValue(v.value));
            }
        }

        return stack[0];
    }

    /**
     * Convert a raw value to its logical type representation.
     */
    private Object convertValue(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        return LogicalTypeConverter.convert(rawValue, column.type(), column.logicalType());
    }

    /**
     * Read the next value with its levels. Uses lookahead if available.
     */
    private ValueWithLevels nextValue() throws IOException {
        if (lookahead != null) {
            ValueWithLevels result = lookahead;
            lookahead = null;
            return result;
        }
        return readFromPage();
    }

    /**
     * Peek at the next repetition level without consuming the value.
     * Returns -1 if no more values (which will fail the > 0 check in callers).
     */
    private int peekRepLevel() throws IOException {
        if (lookahead == null) {
            lookahead = readFromPage();
        }
        return lookahead != null ? lookahead.repLevel : -1;
    }

    /**
     * Read a single value with levels from the current page.
     */
    private ValueWithLevels readFromPage() throws IOException {
        ensurePageLoaded();
        if (currentPage == null || currentPagePosition >= currentPage.numValues()) {
            return null;
        }

        int defLevel = maxDefinitionLevel > 0 ? currentPage.definitionLevels()[currentPagePosition] : 0;
        int repLevel = maxRepetitionLevel > 0 && currentPage.repetitionLevels() != null
                ? currentPage.repetitionLevels()[currentPagePosition]
                : 0;
        Object value = currentPage.values()[currentPagePosition];

        currentPagePosition++;
        valuesRead++;
        return new ValueWithLevels(value, defLevel, repLevel);
    }

    /**
     * Ensure a page is loaded.
     */
    private void ensurePageLoaded() throws IOException {
        if (currentPage == null || currentPagePosition >= currentPage.numValues()) {
            currentPage = pageReader.readPage();
            currentPagePosition = 0;
        }
    }

    private record ValueWithLevels(Object value, int defLevel, int repLevel) {
    }

    /**
     * Get the current page, loading next page if needed.
     * Returns null if no more pages.
     */
    private PageReader.Page getCurrentPage() throws IOException {
        if (currentPage == null || currentPagePosition >= currentPage.numValues()) {
            currentPage = pageReader.readPage();
            currentPagePosition = 0;
        }
        return currentPage;
    }

    /**
     * Read a batch of values from this column.
     * Returns raw values with their definition and repetition levels,
     * allowing the caller to assemble nested structures.
     *
     * @param batchSize maximum number of records to read
     * @return ColumnBatch with values and levels for up to batchSize records
     */
    ColumnBatch readBatch(int batchSize) throws IOException {
        // Use primitive arrays that grow as needed
        int capacity = batchSize * 2;  // Initial capacity, will grow if needed
        Object[] values = new Object[capacity];
        int[] defLevels = new int[capacity];
        int[] repLevels = new int[capacity];
        int valueCount = 0;
        int recordCount = 0;

        // Handle any lookahead value from previous batch
        if (lookahead != null) {
            values[0] = lookahead.value;
            defLevels[0] = lookahead.defLevel;
            repLevels[0] = lookahead.repLevel;
            valueCount = 1;
            lookahead = null;
        }

        while (recordCount < batchSize && hasNext()) {
            PageReader.Page page = getCurrentPage();
            if (page == null) {
                break;
            }

            int pageStart = currentPagePosition;
            int pageEnd = page.numValues();

            // Find how many values to take: scan for record boundaries
            int endPos = pageEnd;
            int recordsFound = 0;

            for (int i = pageStart; i < pageEnd; i++) {
                int rep = page.repetitionLevels() != null ? page.repetitionLevels()[i] : 0;
                if (rep == 0 && (valueCount > 0 || i > pageStart)) {
                    // Start of a new record - check if adding it would exceed batch size
                    if (recordCount + recordsFound + 1 >= batchSize) {
                        endPos = i;  // Don't include this record in current batch
                        break;
                    }
                    recordsFound++;
                }
            }

            int copyCount = endPos - pageStart;

            // Grow arrays if needed
            if (valueCount + copyCount > values.length) {
                int newCapacity = Math.max(values.length * 2, valueCount + copyCount);
                values = Arrays.copyOf(values, newCapacity);
                defLevels = Arrays.copyOf(defLevels, newCapacity);
                repLevels = Arrays.copyOf(repLevels, newCapacity);
            }

            // Copy values directly from page arrays
            System.arraycopy(page.values(), pageStart, values, valueCount, copyCount);
            if (page.definitionLevels() != null) {
                System.arraycopy(page.definitionLevels(), pageStart, defLevels, valueCount, copyCount);
            }
            if (page.repetitionLevels() != null) {
                System.arraycopy(page.repetitionLevels(), pageStart, repLevels, valueCount, copyCount);
            }

            // Count records in copied portion
            for (int i = 0; i < copyCount; i++) {
                int rep = page.repetitionLevels() != null ? page.repetitionLevels()[pageStart + i] : 0;
                if (rep == 0 && (valueCount + i) > 0) {
                    recordCount++;
                }
            }
            // Count the first record if this is the start
            if (valueCount == 0 && copyCount > 0) {
                recordCount++;
            }

            // Update position
            currentPagePosition = endPos;
            valuesRead += copyCount;
            valueCount += copyCount;

            // If we hit the batch limit mid-page, stop
            if (endPos < pageEnd) {
                break;
            }
        }

        // Trim arrays to actual size
        return new ColumnBatch(
                valueCount == values.length ? values : Arrays.copyOf(values, valueCount),
                valueCount == defLevels.length ? defLevels : Arrays.copyOf(defLevels, valueCount),
                valueCount == repLevels.length ? repLevels : Arrays.copyOf(repLevels, valueCount),
                recordCount,
                column);
    }

    /**
     * Read a batch of values with typed primitive data where possible.
     * For flat schemas (no repetition), this returns TypedColumnData with primitive arrays.
     *
     * @param batchSize maximum number of records to read
     * @return ColumnBatch with typed data when available
     */
    ColumnBatch readTypedBatch(int batchSize) throws IOException {
        // For columns with repetition, fall back to Object[] since records can span multiple values
        if (maxRepetitionLevel > 0) {
            return readBatch(batchSize);
        }

        // For flat schemas, each value is one record. Read exactly batchSize records.
        int recordsNeeded = batchSize;
        int recordsCollected = 0;

        // We'll collect slices from pages and combine them
        List<TypedSlice> slices = new ArrayList<>();

        while (recordsCollected < recordsNeeded && hasNext()) {
            // Ensure we have a typed page loaded
            if (currentTypedPage == null || currentTypedPagePosition >= currentTypedPage.numValues()) {
                currentTypedPage = pageReader.readTypedPage();
                currentTypedPagePosition = 0;
                if (currentTypedPage == null) {
                    break;
                }
            }

            // Calculate how many records to take from this page
            int availableInPage = currentTypedPage.numValues() - currentTypedPagePosition;
            int toTake = Math.min(availableInPage, recordsNeeded - recordsCollected);

            // Record this slice
            slices.add(new TypedSlice(currentTypedPage, currentTypedPagePosition, toTake));

            currentTypedPagePosition += toTake;
            valuesRead += toTake;
            recordsCollected += toTake;
        }

        if (slices.isEmpty()) {
            return new ColumnBatch(new Object[0], new int[0], new int[0], 0, column);
        }

        // If single slice covering the whole page, return directly
        if (slices.size() == 1) {
            TypedSlice slice = slices.get(0);
            if (slice.start == 0 && slice.count == slice.page.numValues()) {
                return new ColumnBatch(slice.page.columnData(), slice.page.repetitionLevels(),
                        slice.count, column);
            }
        }

        // Combine slices into a single batch
        return combineTypedSlices(slices, recordsCollected);
    }

    private record TypedSlice(PageReader.TypedPage page, int start, int count) {
    }

    /**
     * Combine typed slices into a single ColumnBatch.
     */
    private ColumnBatch combineTypedSlices(List<TypedSlice> slices, int recordCount) {
        TypedSlice firstSlice = slices.get(0);
        TypedColumnData firstData = firstSlice.page.columnData();
        int maxDefLevel = firstData.maxDefinitionLevel();

        if (firstData instanceof TypedColumnData.LongColumn) {
            long[] combined = new long[recordCount];
            int[] defLevels = new int[recordCount];
            int pos = 0;
            for (TypedSlice slice : slices) {
                TypedColumnData.LongColumn col = (TypedColumnData.LongColumn) slice.page.columnData();
                System.arraycopy(col.values(), slice.start, combined, pos, slice.count);
                if (col.definitionLevels() != null) {
                    System.arraycopy(col.definitionLevels(), slice.start, defLevels, pos, slice.count);
                }
                pos += slice.count;
            }
            TypedColumnData combinedData = new TypedColumnData.LongColumn(
                    combined, defLevels, maxDefLevel, recordCount);
            return new ColumnBatch(combinedData, null, recordCount, column);
        }
        else if (firstData instanceof TypedColumnData.DoubleColumn) {
            double[] combined = new double[recordCount];
            int[] defLevels = new int[recordCount];
            int pos = 0;
            for (TypedSlice slice : slices) {
                TypedColumnData.DoubleColumn col = (TypedColumnData.DoubleColumn) slice.page.columnData();
                System.arraycopy(col.values(), slice.start, combined, pos, slice.count);
                if (col.definitionLevels() != null) {
                    System.arraycopy(col.definitionLevels(), slice.start, defLevels, pos, slice.count);
                }
                pos += slice.count;
            }
            TypedColumnData combinedData = new TypedColumnData.DoubleColumn(
                    combined, defLevels, maxDefLevel, recordCount);
            return new ColumnBatch(combinedData, null, recordCount, column);
        }
        else if (firstData instanceof TypedColumnData.IntColumn) {
            int[] combined = new int[recordCount];
            int[] defLevels = new int[recordCount];
            int pos = 0;
            for (TypedSlice slice : slices) {
                TypedColumnData.IntColumn col = (TypedColumnData.IntColumn) slice.page.columnData();
                System.arraycopy(col.values(), slice.start, combined, pos, slice.count);
                if (col.definitionLevels() != null) {
                    System.arraycopy(col.definitionLevels(), slice.start, defLevels, pos, slice.count);
                }
                pos += slice.count;
            }
            TypedColumnData combinedData = new TypedColumnData.IntColumn(
                    combined, defLevels, maxDefLevel, recordCount);
            return new ColumnBatch(combinedData, null, recordCount, column);
        }
        else {
            // ObjectColumn - combine Object arrays
            Object[] combined = new Object[recordCount];
            int[] defLevels = new int[recordCount];
            int pos = 0;
            for (TypedSlice slice : slices) {
                TypedColumnData.ObjectColumn col = (TypedColumnData.ObjectColumn) slice.page.columnData();
                System.arraycopy(col.values(), slice.start, combined, pos, slice.count);
                if (col.definitionLevels() != null) {
                    System.arraycopy(col.definitionLevels(), slice.start, defLevels, pos, slice.count);
                }
                pos += slice.count;
            }
            return new ColumnBatch(combined, defLevels, null, recordCount, column);
        }
    }

    public ColumnSchema getColumnSchema() {
        return column;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }
}
