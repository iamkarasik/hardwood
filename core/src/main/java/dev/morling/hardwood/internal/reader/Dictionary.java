/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

/**
 * Typed dictionary for dictionary-encoded Parquet columns.
 * Each variant holds a primitive array of dictionary values.
 */
public sealed interface Dictionary {

    int size();

    record IntDictionary(int[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }
    }

    record LongDictionary(long[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }
    }

    record FloatDictionary(float[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }
    }

    record DoubleDictionary(double[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }
    }

    record ByteArrayDictionary(byte[][] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }
    }
}
