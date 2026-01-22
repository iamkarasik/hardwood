/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reads column values from pages and prefetches them into batches.
 *
 * <p>This class fetches pages from the PageReader and provides batch prefetching
 * for efficient columnar access. For flat schemas, use {@link #prefetchTypedFlat(int)}
 * which copies directly into typed primitive arrays. For nested schemas, use
 * {@link #prefetch(int)} which handles repetition levels.</p>
 */
public class ColumnValueIterator {

    private final PageReader pageReader;
    private final ColumnSchema column;
    private final int maxDefinitionLevel;
    private final long totalValues;

    private Page currentPage;
    private int position;
    private int currentRecordStart;
    private long valuesRead;
    private boolean recordActive;

    public ColumnValueIterator(PageReader pageReader, ColumnSchema column, long totalValues) {
        this.pageReader = pageReader;
        this.column = column;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.totalValues = totalValues;
    }

    // ==================== Batch Prefetch ====================

    /**
     * Prefetch values for up to {@code maxRecords} records into typed arrays.
     *
     * @param maxRecords maximum number of records to prefetch
     * @return typed column data containing values, levels, and record boundaries
     */
    public TypedColumnData prefetch(int maxRecords) {
        if (column.maxRepetitionLevel() == 0) {
            return prefetchTypedFlat(maxRecords);
        }
        return prefetchNested(maxRecords);
    }

    /**
     * Prefetch values for flat columns into typed primitive arrays.
     * This avoids boxing overhead for primitive types (int, long, double, etc.).
     * <p>
     * Only valid for columns with maxRepetitionLevel == 0 (non-repeated columns).
     * </p>
     *
     * @param maxRecords maximum number of records to prefetch
     * @return typed prefetched column data with primitive arrays
     * @throws IllegalStateException if the column has repetition (nested/repeated)
     */
    public TypedColumnData prefetchTypedFlat(int maxRecords) {
        if (column.maxRepetitionLevel() != 0) {
            throw new IllegalStateException("prefetchTypedFlat() only valid for flat columns, " +
                    "column " + column.name() + " has maxRepetitionLevel=" + column.maxRepetitionLevel());
        }

        int maxDefLevel = column.maxDefinitionLevel();

        // Dispatch based on current page type to determine which typed array to use
        if (!ensurePageLoaded()) {
            // No data available - return empty typed column based on physical type
            return switch (column.type()) {
                case INT32 -> new TypedColumnData.IntColumn(column, new int[0], new int[0], maxDefLevel, 0);
                case INT64 -> new TypedColumnData.LongColumn(column, new long[0], new int[0], maxDefLevel, 0);
                case FLOAT -> new TypedColumnData.FloatColumn(column, new float[0], new int[0], maxDefLevel, 0);
                case DOUBLE -> new TypedColumnData.DoubleColumn(column, new double[0], new int[0], maxDefLevel, 0);
                case BOOLEAN -> new TypedColumnData.BooleanColumn(column, new boolean[0], new int[0], maxDefLevel, 0);
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new TypedColumnData.ByteArrayColumn(column, new byte[0][], new int[0], maxDefLevel, 0);
            };
        }

        return switch (currentPage) {
            case Page.IntPage p -> prefetchTypedInt(maxRecords, maxDefLevel);
            case Page.LongPage p -> prefetchTypedLong(maxRecords, maxDefLevel);
            case Page.FloatPage p -> prefetchTypedFloat(maxRecords, maxDefLevel);
            case Page.DoublePage p -> prefetchTypedDouble(maxRecords, maxDefLevel);
            case Page.BooleanPage p -> prefetchTypedBoolean(maxRecords, maxDefLevel);
            case Page.ByteArrayPage p -> prefetchTypedByteArray(maxRecords, maxDefLevel);
        };
    }

    private TypedColumnData prefetchTypedInt(int maxRecords, int maxDefLevel) {
        int[] values = new int[maxRecords];
        int[] defLevels = maxDefLevel > 0 ? new int[maxRecords] : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.IntPage page = (Page.IntPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            if (defLevels != null) {
                System.arraycopy(page.definitionLevels(), position, defLevels, recordCount, toCopy);
            }

            position += toCopy;
            valuesRead += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = java.util.Arrays.copyOf(values, recordCount);
            if (defLevels != null) {
                defLevels = java.util.Arrays.copyOf(defLevels, recordCount);
            }
        }

        return new TypedColumnData.IntColumn(column, values, defLevels, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchTypedLong(int maxRecords, int maxDefLevel) {
        long[] values = new long[maxRecords];
        int[] defLevels = maxDefLevel > 0 ? new int[maxRecords] : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.LongPage page = (Page.LongPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            if (defLevels != null) {
                System.arraycopy(page.definitionLevels(), position, defLevels, recordCount, toCopy);
            }

            position += toCopy;
            valuesRead += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = java.util.Arrays.copyOf(values, recordCount);
            if (defLevels != null) {
                defLevels = java.util.Arrays.copyOf(defLevels, recordCount);
            }
        }

        return new TypedColumnData.LongColumn(column, values, defLevels, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchTypedFloat(int maxRecords, int maxDefLevel) {
        float[] values = new float[maxRecords];
        int[] defLevels = maxDefLevel > 0 ? new int[maxRecords] : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.FloatPage page = (Page.FloatPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            if (defLevels != null) {
                System.arraycopy(page.definitionLevels(), position, defLevels, recordCount, toCopy);
            }

            position += toCopy;
            valuesRead += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = java.util.Arrays.copyOf(values, recordCount);
            if (defLevels != null) {
                defLevels = java.util.Arrays.copyOf(defLevels, recordCount);
            }
        }

        return new TypedColumnData.FloatColumn(column, values, defLevels, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchTypedDouble(int maxRecords, int maxDefLevel) {
        double[] values = new double[maxRecords];
        int[] defLevels = maxDefLevel > 0 ? new int[maxRecords] : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.DoublePage page = (Page.DoublePage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            if (defLevels != null) {
                System.arraycopy(page.definitionLevels(), position, defLevels, recordCount, toCopy);
            }

            position += toCopy;
            valuesRead += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = java.util.Arrays.copyOf(values, recordCount);
            if (defLevels != null) {
                defLevels = java.util.Arrays.copyOf(defLevels, recordCount);
            }
        }

        return new TypedColumnData.DoubleColumn(column, values, defLevels, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchTypedBoolean(int maxRecords, int maxDefLevel) {
        boolean[] values = new boolean[maxRecords];
        int[] defLevels = maxDefLevel > 0 ? new int[maxRecords] : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.BooleanPage page = (Page.BooleanPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            if (defLevels != null) {
                System.arraycopy(page.definitionLevels(), position, defLevels, recordCount, toCopy);
            }

            position += toCopy;
            valuesRead += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = java.util.Arrays.copyOf(values, recordCount);
            if (defLevels != null) {
                defLevels = java.util.Arrays.copyOf(defLevels, recordCount);
            }
        }

        return new TypedColumnData.BooleanColumn(column, values, defLevels, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchTypedByteArray(int maxRecords, int maxDefLevel) {
        byte[][] values = new byte[maxRecords][];
        int[] defLevels = maxDefLevel > 0 ? new int[maxRecords] : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.ByteArrayPage page = (Page.ByteArrayPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            if (defLevels != null) {
                System.arraycopy(page.definitionLevels(), position, defLevels, recordCount, toCopy);
            }

            position += toCopy;
            valuesRead += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = java.util.Arrays.copyOf(values, recordCount);
            if (defLevels != null) {
                defLevels = java.util.Arrays.copyOf(defLevels, recordCount);
            }
        }

        return new TypedColumnData.ByteArrayColumn(column, values, defLevels, maxDefLevel, recordCount);
    }

    /**
     * Prefetch for nested/repeated columns: variable values per record.
     * Dispatches to type-specific methods for typed array access.
     */
    private TypedColumnData prefetchNested(int maxRecords) {
        int maxDefLevel = column.maxDefinitionLevel();

        if (!ensurePageLoaded()) {
            // No data available - return empty typed column based on physical type
            return switch (column.type()) {
                case INT32 -> new TypedColumnData.IntColumn(column, new int[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case INT64 -> new TypedColumnData.LongColumn(column, new long[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case FLOAT -> new TypedColumnData.FloatColumn(column, new float[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case DOUBLE -> new TypedColumnData.DoubleColumn(column, new double[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case BOOLEAN -> new TypedColumnData.BooleanColumn(column, new boolean[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new TypedColumnData.ByteArrayColumn(column, new byte[0][], new int[0], new int[0], new int[0], maxDefLevel, 0);
            };
        }

        return switch (currentPage) {
            case Page.IntPage p -> prefetchNestedInt(maxRecords, maxDefLevel);
            case Page.LongPage p -> prefetchNestedLong(maxRecords, maxDefLevel);
            case Page.FloatPage p -> prefetchNestedFloat(maxRecords, maxDefLevel);
            case Page.DoublePage p -> prefetchNestedDouble(maxRecords, maxDefLevel);
            case Page.BooleanPage p -> prefetchNestedBoolean(maxRecords, maxDefLevel);
            case Page.ByteArrayPage p -> prefetchNestedByteArray(maxRecords, maxDefLevel);
        };
    }

    private TypedColumnData prefetchNestedInt(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        int[] values = new int[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = java.util.Arrays.copyOf(values, newSize);
                    defLevels = java.util.Arrays.copyOf(defLevels, newSize);
                    repLevels = java.util.Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.IntPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = java.util.Arrays.copyOf(values, valueCount);
            defLevels = java.util.Arrays.copyOf(defLevels, valueCount);
            repLevels = java.util.Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = java.util.Arrays.copyOf(recordOffsets, recordCount);
        }

        return new TypedColumnData.IntColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchNestedLong(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        long[] values = new long[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = java.util.Arrays.copyOf(values, newSize);
                    defLevels = java.util.Arrays.copyOf(defLevels, newSize);
                    repLevels = java.util.Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.LongPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = java.util.Arrays.copyOf(values, valueCount);
            defLevels = java.util.Arrays.copyOf(defLevels, valueCount);
            repLevels = java.util.Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = java.util.Arrays.copyOf(recordOffsets, recordCount);
        }

        return new TypedColumnData.LongColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchNestedFloat(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        float[] values = new float[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = java.util.Arrays.copyOf(values, newSize);
                    defLevels = java.util.Arrays.copyOf(defLevels, newSize);
                    repLevels = java.util.Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.FloatPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = java.util.Arrays.copyOf(values, valueCount);
            defLevels = java.util.Arrays.copyOf(defLevels, valueCount);
            repLevels = java.util.Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = java.util.Arrays.copyOf(recordOffsets, recordCount);
        }

        return new TypedColumnData.FloatColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchNestedDouble(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        double[] values = new double[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = java.util.Arrays.copyOf(values, newSize);
                    defLevels = java.util.Arrays.copyOf(defLevels, newSize);
                    repLevels = java.util.Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.DoublePage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = java.util.Arrays.copyOf(values, valueCount);
            defLevels = java.util.Arrays.copyOf(defLevels, valueCount);
            repLevels = java.util.Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = java.util.Arrays.copyOf(recordOffsets, recordCount);
        }

        return new TypedColumnData.DoubleColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchNestedBoolean(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        boolean[] values = new boolean[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = java.util.Arrays.copyOf(values, newSize);
                    defLevels = java.util.Arrays.copyOf(defLevels, newSize);
                    repLevels = java.util.Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.BooleanPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = java.util.Arrays.copyOf(values, valueCount);
            defLevels = java.util.Arrays.copyOf(defLevels, valueCount);
            repLevels = java.util.Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = java.util.Arrays.copyOf(recordOffsets, recordCount);
        }

        return new TypedColumnData.BooleanColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData prefetchNestedByteArray(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        byte[][] values = new byte[estimatedValues][];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = java.util.Arrays.copyOf(values, newSize);
                    defLevels = java.util.Arrays.copyOf(defLevels, newSize);
                    repLevels = java.util.Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.ByteArrayPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = java.util.Arrays.copyOf(values, valueCount);
            defLevels = java.util.Arrays.copyOf(defLevels, valueCount);
            repLevels = java.util.Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = java.util.Arrays.copyOf(recordOffsets, recordCount);
        }

        return new TypedColumnData.ByteArrayColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    // ==================== Internal Helpers ====================

    private boolean ensurePageLoaded() {
        if (currentPage != null && position < currentPage.size()) {
            return true;
        }

        if (valuesRead >= totalValues) {
            return false;
        }

        try {
            currentPage = pageReader.readPage();
            position = 0;
            return currentPage != null;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read page", e);
        }
    }

    private boolean nextRecord() {
        if (!ensurePageLoaded()) {
            recordActive = false;
            return false;
        }

        currentRecordStart = position;
        recordActive = true;
        return true;
    }

    private boolean hasValue() {
        if (!recordActive || !ensurePageLoaded()) {
            return false;
        }

        if (position == currentRecordStart) {
            return true;
        }

        int[] repLevels = currentPage.repetitionLevels();
        return repLevels != null && repLevels[position] > 0;
    }

    private int repetitionLevel() {
        int[] repLevels = currentPage.repetitionLevels();
        return repLevels != null ? repLevels[position] : 0;
    }

    private int definitionLevel() {
        int[] defLevels = currentPage.definitionLevels();
        return defLevels != null ? defLevels[position] : maxDefinitionLevel;
    }

    private void advance() {
        position++;
        valuesRead++;

        if (currentPage != null && position >= currentPage.size()) {
            try {
                currentPage = pageReader.readPage();
                position = 0;
                if (recordActive && currentPage != null) {
                    int[] repLevels = currentPage.repetitionLevels();
                    if (repLevels != null && repLevels[0] > 0) {
                        currentRecordStart = 0;
                    }
                    else {
                        currentRecordStart = -1;
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to read next page", e);
            }
        }
    }
}
