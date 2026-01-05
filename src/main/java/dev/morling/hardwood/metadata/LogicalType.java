/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

/**
 * Logical types that provide semantic meaning to physical types.
 * For Milestone 1, we support basic types only.
 */
public enum LogicalType {
    STRING,
    ENUM,
    UUID,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    INTERVAL,
    JSON,
    BSON,
    LIST,
    MAP,
    MAP_KEY_VALUE;
}
