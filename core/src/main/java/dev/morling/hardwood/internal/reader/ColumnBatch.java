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
 * <p>Usage:</p>
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
    private final int[] definitionLevels;
    private final int[] repetitionLevels;
    private final int recordCount;
    private final ColumnSchema column;

    private int position = -1;      // Current value position (-1 = before first record)
    private int recordEnd = 0;      // End position of current record
    private int recordsRead = 0;    // Number of records consumed

    public ColumnBatch(Object[] values, int[] definitionLevels, int[] repetitionLevels,
                       int recordCount, ColumnSchema column) {
        this.values = values;
        this.definitionLevels = definitionLevels;
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
     * Advance to the next record. Returns false if no more records.
     */
    public boolean nextRecord() {
        if (recordsRead >= recordCount) {
            return false;
        }
        position = recordEnd;
        recordEnd = findRecordEnd(position);
        recordsRead++;
        return true;
    }

    private int findRecordEnd(int start) {
        for (int i = start + 1; i < values.length; i++) {
            if (repetitionLevels[i] == 0) {
                return i;
            }
        }
        return values.length;
    }

    /**
     * True if there are more values in the current record.
     */
    public boolean hasValue() {
        return position < recordEnd;
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
