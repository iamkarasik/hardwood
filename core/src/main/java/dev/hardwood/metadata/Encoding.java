/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Encoding types for Parquet data.
 */
public enum Encoding {
    PLAIN,
    PLAIN_DICTIONARY,
    RLE,
    BIT_PACKED,
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    RLE_DICTIONARY,
    BYTE_STREAM_SPLIT,
    /**
     * Placeholder for unknown/unsupported encodings found in metadata.
     * This allows reading files that list encodings we don't recognize
     * in the column metadata, as long as the actual pages use supported encodings.
     */
    UNKNOWN
}
