/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Field repetition types in Parquet schema.
 */
public enum RepetitionType {
    REQUIRED, // Field must be present
    OPTIONAL, // Field may be null
    REPEATED  // Field can appear multiple times (list)
}
