/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

/**
 * Sealed interface for typed column storage with primitive arrays.
 * <p>
 * This eliminates boxing overhead for primitive types by storing values
 * directly in primitive arrays (long[], double[], int[]) instead of Object[].
 * </p>
 */
public sealed interface TypedColumnData {

    int size();

    int maxDefinitionLevel();

    int[] definitionLevels();

    default boolean isNull(int index) {
        int[] defLevels = definitionLevels();
        if (defLevels == null) {
            return false;
        }
        return defLevels[index] < maxDefinitionLevel();
    }

    record LongColumn(long[] values, int[] definitionLevels, int maxDefinitionLevel, int size)
            implements TypedColumnData {
        public long get(int index) {
            return values[index];
        }
    }

    record DoubleColumn(double[] values, int[] definitionLevels, int maxDefinitionLevel, int size)
            implements TypedColumnData {
        public double get(int index) {
            return values[index];
        }
    }

    record IntColumn(int[] values, int[] definitionLevels, int maxDefinitionLevel, int size)
            implements TypedColumnData {
        public int get(int index) {
            return values[index];
        }
    }

    record ObjectColumn(Object[] values, int[] definitionLevels, int maxDefinitionLevel, int size)
            implements TypedColumnData {
        public Object get(int index) {
            return values[index];
        }
    }
}
