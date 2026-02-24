/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Physical types supported by Parquet format.
 * These represent how data is stored on disk.
 */
public enum PhysicalType {
    BOOLEAN,
    INT32,
    INT64,
    INT96, // Deprecated, used for legacy timestamp
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY
}
