/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

/**
 * Encoding types for Parquet data.
 */
public enum Encoding {
    PLAIN(0),
    PLAIN_DICTIONARY(2),
    RLE(3),
    BIT_PACKED(4),
    DELTA_BINARY_PACKED(5),
    DELTA_LENGTH_BYTE_ARRAY(6),
    DELTA_BYTE_ARRAY(7),
    RLE_DICTIONARY(8),
    BYTE_STREAM_SPLIT(9);

    private final int thriftValue;

    Encoding(int thriftValue) {
        this.thriftValue = thriftValue;
    }

    public int getThriftValue() {
        return thriftValue;
    }

    public static Encoding fromThriftValue(int value) {
        for (Encoding encoding : values()) {
            if (encoding.thriftValue == value) {
                return encoding;
            }
        }
        throw new IllegalArgumentException("Unknown encoding: " + value);
    }
}
