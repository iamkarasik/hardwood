/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.encoding;

import java.io.IOException;

/**
 * Interface for decoders that read values directly into an output array,
 * placing them at positions indicated by definition levels.
 */
public interface ValueDecoder {

    /**
     * Initialize the decoder with the number of non-null values to read.
     * Some decoders (e.g., delta encodings) need to read header information
     * before decoding individual values.
     *
     * @param numNonNullValues the number of actual values in the encoded data
     */
    default void initialize(int numNonNullValues) throws IOException {
        // No-op by default - most decoders don't need initialization
    }

    /**
     * Read values directly into output array at positions indicated by definition levels.
     *
     * @param output the output array to populate
     * @param definitionLevels definition levels indicating which positions have values (null for required columns)
     * @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
     */
    void readValues(Object[] output, int[] definitionLevels, int maxDefLevel) throws IOException;

    /**
     * Read long values directly into a primitive array.
     *
     * @param output the output array to populate
     * @param definitionLevels definition levels indicating which positions have values (null for required columns)
     * @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
     */
    default void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        throw new UnsupportedOperationException("readLongs not supported by this decoder");
    }

    /**
     * Read double values directly into a primitive array.
     *
     * @param output the output array to populate
     * @param definitionLevels definition levels indicating which positions have values (null for required columns)
     * @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
     */
    default void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        throw new UnsupportedOperationException("readDoubles not supported by this decoder");
    }

    /**
     * Read int values directly into a primitive array.
     *
     * @param output the output array to populate
     * @param definitionLevels definition levels indicating which positions have values (null for required columns)
     * @param maxDefLevel the maximum definition level (value is present when defLevel == maxDefLevel)
     */
    default void readInts(int[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        throw new UnsupportedOperationException("readInts not supported by this decoder");
    }
}
