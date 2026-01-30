/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.BitSet;

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

    /** Pre-computed null flags for fast null checks. Null if column is non-nullable. */
    BitSet nulls();

    /** Get the value at index, boxing primitives. Used by RecordAssembler for nested schemas. */
    Object getValue(int index);

    default boolean isNull(int index) {
        BitSet n = nulls();
        return n != null && n.get(index);
    }

    /**
     * Compute null flags from definition levels.
     * Returns null if the column has no nulls (maxDefLevel == 0).
     */
    static BitSet computeNulls(int[] definitionLevels, int maxDefinitionLevel, int valueCount) {
        if (maxDefinitionLevel == 0) {
            return null;
        }
        BitSet nulls = new BitSet(valueCount);
        if (definitionLevels == null) {
            // All nulls if no definition levels but maxDefLevel > 0
            nulls.set(0, valueCount);
        }
        else {
            for (int i = 0; i < valueCount; i++) {
                if (definitionLevels[i] < maxDefinitionLevel) {
                    nulls.set(i);
                }
            }
        }
        return nulls;
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
                     int[] recordOffsets, BitSet nulls, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        /** Convenience constructor for flat schemas. */
        public IntColumn(ColumnSchema column, int[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
        }

        /** Convenience constructor for nested schemas. */
        public IntColumn(ColumnSchema column, int[] values, int[] definitionLevels, int[] repetitionLevels,
                         int[] recordOffsets, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, repetitionLevels, recordOffsets,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
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
                      int[] recordOffsets, BitSet nulls, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        /** Convenience constructor for flat schemas. */
        public LongColumn(ColumnSchema column, long[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
        }

        /** Convenience constructor for nested schemas. */
        public LongColumn(ColumnSchema column, long[] values, int[] definitionLevels, int[] repetitionLevels,
                          int[] recordOffsets, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, repetitionLevels, recordOffsets,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
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
                       int[] recordOffsets, BitSet nulls, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        /** Convenience constructor for flat schemas. */
        public FloatColumn(ColumnSchema column, float[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
        }

        /** Convenience constructor for nested schemas. */
        public FloatColumn(ColumnSchema column, float[] values, int[] definitionLevels, int[] repetitionLevels,
                           int[] recordOffsets, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, repetitionLevels, recordOffsets,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
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
                        int[] recordOffsets, BitSet nulls, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        /** Convenience constructor for flat schemas. */
        public DoubleColumn(ColumnSchema column, double[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
        }

        /** Convenience constructor for nested schemas. */
        public DoubleColumn(ColumnSchema column, double[] values, int[] definitionLevels, int[] repetitionLevels,
                            int[] recordOffsets, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, repetitionLevels, recordOffsets,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
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
                         int[] recordOffsets, BitSet nulls, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        /** Convenience constructor for flat schemas. */
        public BooleanColumn(ColumnSchema column, boolean[] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
        }

        /** Convenience constructor for nested schemas. */
        public BooleanColumn(ColumnSchema column, boolean[] values, int[] definitionLevels, int[] repetitionLevels,
                             int[] recordOffsets, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, repetitionLevels, recordOffsets,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
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
                           int[] recordOffsets, BitSet nulls, int maxDefinitionLevel, int recordCount) implements TypedColumnData {
        /** Convenience constructor for flat schemas. */
        public ByteArrayColumn(ColumnSchema column, byte[][] values, int[] definitionLevels, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, null, null,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
        }

        /** Convenience constructor for nested schemas. */
        public ByteArrayColumn(ColumnSchema column, byte[][] values, int[] definitionLevels, int[] repetitionLevels,
                               int[] recordOffsets, int maxDefinitionLevel, int recordCount) {
            this(column, values, definitionLevels, repetitionLevels, recordOffsets,
                    computeNulls(definitionLevels, maxDefinitionLevel, values.length),
                    maxDefinitionLevel, recordCount);
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
