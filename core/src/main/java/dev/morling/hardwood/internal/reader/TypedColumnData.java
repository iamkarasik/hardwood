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
 * Sealed interface for typed column data with primitive arrays.
 * <p>
 * Stores values in typed primitive arrays to eliminate boxing overhead.
 * Supports both flat schemas (direct columnar access) and nested schemas
 * (with repetition levels and record offsets for RecordAssembler).
 * </p>
 */
public sealed interface TypedColumnData {

    ColumnSchema column();

    int recordCount();

    int maxDefinitionLevel();

    int[] definitionLevels();

    /** Repetition levels for nested schemas, null for flat schemas. */
    int[] repetitionLevels();

    /** Record offsets for nested schemas, null for flat schemas. */
    int[] recordOffsets();

    /** Get the value at index, boxing primitives. Used by RecordAssembler for nested schemas. */
    Object getValue(int index);

    default boolean isNull(int index) {
        int maxDefLevel = maxDefinitionLevel();
        if (maxDefLevel == 0) {
            return false;
        }
        int[] defLevels = definitionLevels();
        return defLevels == null || defLevels[index] < maxDefLevel;
    }

    default int getDefLevel(int index) {
        int[] defLevels = definitionLevels();
        return defLevels != null ? defLevels[index] : maxDefinitionLevel();
    }

    default int getRepLevel(int index) {
        int[] repLevels = repetitionLevels();
        return repLevels != null ? repLevels[index] : 0;
    }

    default int getStartOffset(int recordIndex) {
        int[] offsets = recordOffsets();
        return offsets != null ? offsets[recordIndex] : recordIndex;
    }

    default int getValueCount(int recordIndex) {
        int[] offsets = recordOffsets();
        if (offsets == null) {
            return 1; // Flat schema: one value per record
        }
        int start = offsets[recordIndex];
        int end = (recordIndex + 1 < recordCount()) ? offsets[recordIndex + 1] : valueCount();
        return end - start;
    }

    /** Total number of values (may differ from recordCount for nested schemas). */
    int valueCount();

    record IntColumn(ColumnSchema column, int[] values, int[] definitionLevels, int[] repetitionLevels,
                     int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        public IntColumn(ColumnSchema column, int[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null, maxDefinitionLevel, recordCount);
        }

        public int get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record LongColumn(ColumnSchema column, long[] values, int[] definitionLevels, int[] repetitionLevels,
                      int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        public LongColumn(ColumnSchema column, long[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null, maxDefinitionLevel, recordCount);
        }

        public long get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record FloatColumn(ColumnSchema column, float[] values, int[] definitionLevels, int[] repetitionLevels,
                       int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        public FloatColumn(ColumnSchema column, float[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null, maxDefinitionLevel, recordCount);
        }

        public float get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record DoubleColumn(ColumnSchema column, double[] values, int[] definitionLevels, int[] repetitionLevels,
                        int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        public DoubleColumn(ColumnSchema column, double[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null, maxDefinitionLevel, recordCount);
        }

        public double get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record BooleanColumn(ColumnSchema column, boolean[] values, int[] definitionLevels, int[] repetitionLevels,
                         int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        public BooleanColumn(ColumnSchema column, boolean[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null, maxDefinitionLevel, recordCount);
        }

        public boolean get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record ByteArrayColumn(ColumnSchema column, byte[][] values, int[] definitionLevels, int[] repetitionLevels,
                           int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        public ByteArrayColumn(ColumnSchema column, byte[][] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null, maxDefinitionLevel, recordCount);
        }

        public byte[] get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }
}
