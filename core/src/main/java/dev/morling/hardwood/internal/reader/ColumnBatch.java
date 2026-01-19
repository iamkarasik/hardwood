/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * A cursor over column values with definition and repetition levels.
 * Iterates record by record, with each record containing one or more values.
 *
 * <h2>Relationship to Parquet Structure</h2>
 *
 * <p>In Parquet, data is organized as:</p>
 * <ul>
 *   <li><b>Row Group</b> - horizontal partition containing a subset of rows for all columns</li>
 *   <li><b>Column Chunk</b> - all data for one column within one row group</li>
 *   <li><b>Page</b> - smallest storage unit (~1MB); column chunks contain multiple pages</li>
 * </ul>
 *
 * <p>A {@code ColumnBatch} is a Hardwood abstraction that reads a fixed number of
 * <em>complete records</em> from a column chunk. Key properties:</p>
 * <ul>
 *   <li>A batch may span multiple pages (pages are just compression boundaries)</li>
 *   <li>Records are never split across batches - each batch contains only complete records</li>
 *   <li>For nested/repeated fields, all values belonging to one record are in the same batch</li>
 *   <li>All column batches for one read cycle have the same record count</li>
 * </ul>
 *
 * <p>This guarantees that {@link RecordAssembler} can assemble rows without handling
 * partial records across batch boundaries.</p>
 *
 * <h2>Usage</h2>
 * <pre>
 * while (batch.nextRecord()) {
 *     while (batch.hasValue()) {
 *         int r = batch.repetitionLevel();
 *         int d = batch.definitionLevel();
 *         Object v = batch.value();
 *         batch.advance();
 *     }
 * }
 * </pre>
 */
public final class ColumnBatch {

    private final Object[] values;
    private final TypedColumnData typedData;
    private final int[] definitionLevels;
    private final int[] repetitionLevels;
    private final int recordCount;
    private final ColumnSchema column;

    private int position = 0;              // Next value to examine
    private int currentRecordStart = -1;   // Start position of current record (-1 = before first record)
    private int recordsRead = 0;           // Number of records consumed

    public ColumnBatch(Object[] values, int[] definitionLevels, int[] repetitionLevels,
                       int recordCount, ColumnSchema column) {
        this.values = values;
        this.typedData = null;
        this.definitionLevels = definitionLevels;
        this.repetitionLevels = repetitionLevels;
        this.recordCount = recordCount;
        this.column = column;
    }

    /**
     * Create a ColumnBatch with typed primitive data.
     */
    public ColumnBatch(TypedColumnData typedData, int[] repetitionLevels,
                       int recordCount, ColumnSchema column) {
        this.values = null;
        this.typedData = typedData;
        this.definitionLevels = typedData.definitionLevels();
        this.repetitionLevels = repetitionLevels;
        this.recordCount = recordCount;
        this.column = column;
    }

    /**
     * Number of records in this batch.
     */
    public int size() {
        return recordCount;
    }

    /**
     * The column this batch belongs to.
     */
    public ColumnSchema getColumn() {
        return column;
    }

    /**
     * Direct access to the values array for columnar access.
     * For flat schemas, index i corresponds to row i.
     */
    public Object[] getValues() {
        return values;
    }

    /**
     * Direct access to the definition levels array for columnar access.
     * For flat schemas, index i corresponds to row i.
     */
    public int[] getDefinitionLevels() {
        return definitionLevels;
    }

    /**
     * Check if this batch has typed primitive data.
     */
    public boolean hasTypedData() {
        return typedData != null;
    }

    /**
     * Get the typed column data (may be null if not available).
     */
    public TypedColumnData getTypedData() {
        return typedData;
    }

    /**
     * Get a long value at the given index. Only valid when hasTypedData() is true
     * and the column type is INT64.
     */
    public long getLongValue(int index) {
        return ((TypedColumnData.LongColumn) typedData).get(index);
    }

    /**
     * Get a double value at the given index. Only valid when hasTypedData() is true
     * and the column type is DOUBLE.
     */
    public double getDoubleValue(int index) {
        return ((TypedColumnData.DoubleColumn) typedData).get(index);
    }

    /**
     * Get an int value at the given index. Only valid when hasTypedData() is true
     * and the column type is INT32.
     */
    public int getIntValue(int index) {
        return ((TypedColumnData.IntColumn) typedData).get(index);
    }

    /**
     * Advance to the next record. Returns false if no more records.
     */
    public boolean nextRecord() {
        if (recordsRead >= recordCount) {
            return false;
        }

        // Skip remaining values of current record (if caller didn't consume all)
        if (currentRecordStart >= 0 && position > currentRecordStart) {
            while (position < values.length && repetitionLevels[position] > 0) {
                position++;
            }
        }

        currentRecordStart = position;
        recordsRead++;
        return true;
    }

    /**
     * True if there are more values in the current record.
     * Record boundary is detected lazily by checking repetition level:
     * rep=0 means start of a new record, rep>0 means continuation.
     */
    public boolean hasValue() {
        return position < values.length &&
                (position == currentRecordStart || repetitionLevels[position] > 0);
    }

    /**
     * Repetition level of the current value.
     */
    public int repetitionLevel() {
        return repetitionLevels[position];
    }

    /**
     * Definition level of the current value.
     */
    public int definitionLevel() {
        return definitionLevels[position];
    }

    /**
     * The current value.
     */
    public Object value() {
        return values[position];
    }

    /**
     * Advance to the next value within the current record.
     */
    public void advance() {
        position++;
    }

    @Override
    public String toString() {
        return "ColumnBatch[column=" + column.name() +
                ", records=" + recordCount +
                ", values=" + values.length +
                ", position=" + position +
                ", recordsRead=" + recordsRead + "]";
    }
}
