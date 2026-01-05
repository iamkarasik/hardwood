/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.schema;

import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RepetitionType;

/**
 * Represents a column in a flat Parquet schema.
 */
public record ColumnSchema(
        String name,
        PhysicalType type,
        RepetitionType repetitionType,
        Integer typeLength,
        int columnIndex) {

    public int getMaxDefinitionLevel() {
        // For flat schemas:
        // REQUIRED: 0 (no definition level needed)
        // OPTIONAL: 1 (0 = null, 1 = present)
        return repetitionType == RepetitionType.REQUIRED ? 0 : 1;
    }

    public int getMaxRepetitionLevel() {
        // For flat schemas (no repeated fields in Milestone 1)
        return 0;
    }

    public boolean isRequired() {
        return repetitionType == RepetitionType.REQUIRED;
    }

    public boolean isOptional() {
        return repetitionType == RepetitionType.OPTIONAL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(repetitionType.name().toLowerCase());
        sb.append(" ");
        sb.append(type.name().toLowerCase());
        if (typeLength != null) {
            sb.append("(").append(typeLength).append(")");
        }
        sb.append(" ");
        sb.append(name);
        sb.append(";");
        return sb.toString();
    }
}
