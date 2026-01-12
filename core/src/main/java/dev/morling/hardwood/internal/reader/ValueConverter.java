/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Shared validation and conversion logic for PqRow, PqList, and PqMap implementations.
 */
final class ValueConverter {

    private ValueConverter() {
    }

    /**
     * Validate schema and convert a raw value to the requested PqType.
     *
     * @param rawValue the raw value from the Parquet file
     * @param type the requested PqType
     * @param schema the schema node for the field
     * @return the converted value
     * @throws IllegalArgumentException if the schema doesn't match the requested type
     */
    @SuppressWarnings("unchecked")
    static <T> T convert(Object rawValue, PqType<T> type, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }

        // Validate and convert in one pass
        return (T) switch (type) {
            case PqType.BooleanType t -> {
                validatePhysicalType(schema, PhysicalType.BOOLEAN);
                yield rawValue;
            }
            case PqType.Int32Type t -> {
                validatePhysicalType(schema, PhysicalType.INT32);
                yield rawValue;
            }
            case PqType.Int64Type t -> {
                validatePhysicalType(schema, PhysicalType.INT64);
                yield rawValue;
            }
            case PqType.FloatType t -> {
                validatePhysicalType(schema, PhysicalType.FLOAT);
                yield rawValue;
            }
            case PqType.DoubleType t -> {
                validatePhysicalType(schema, PhysicalType.DOUBLE);
                yield rawValue;
            }
            case PqType.BinaryType t -> {
                validatePhysicalType(schema, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
                yield rawValue;
            }
            case PqType.StringType t -> {
                validateStringType(schema);
                yield convertToString(rawValue);
            }
            case PqType.DateType t -> {
                validateLogicalType(schema, LogicalType.DateType.class);
                yield convertLogicalType(rawValue, schema, LocalDate.class);
            }
            case PqType.TimeType t -> {
                validateLogicalType(schema, LogicalType.TimeType.class);
                yield convertLogicalType(rawValue, schema, LocalTime.class);
            }
            case PqType.TimestampType t -> {
                validateLogicalType(schema, LogicalType.TimestampType.class);
                yield convertLogicalType(rawValue, schema, Instant.class);
            }
            case PqType.DecimalType t -> {
                validateLogicalType(schema, LogicalType.DecimalType.class);
                yield convertLogicalType(rawValue, schema, BigDecimal.class);
            }
            case PqType.UuidType t -> {
                validateLogicalType(schema, LogicalType.UuidType.class);
                yield convertLogicalType(rawValue, schema, UUID.class);
            }
            case PqType.RowType t -> {
                validateGroupType(schema, false, false);
                yield new PqRowImpl((Object[]) rawValue, (SchemaNode.GroupNode) schema);
            }
            case PqType.ListType t -> {
                validateGroupType(schema, true, false);
                yield new PqListImpl((List<?>) rawValue, (SchemaNode.GroupNode) schema);
            }
            case PqType.MapType t -> {
                validateGroupType(schema, false, true);
                yield new PqMapImpl((List<?>) rawValue, (SchemaNode.GroupNode) schema);
            }
        };
    }

    private static void validatePhysicalType(SchemaNode schema, PhysicalType... expectedTypes) {
        if (!(schema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a primitive type");
        }
        for (PhysicalType expected : expectedTypes) {
            if (primitive.type() == expected) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "Field '" + schema.name() + "' has physical type " + primitive.type()
                        + ", expected one of " + Arrays.toString(expectedTypes));
    }

    private static void validateStringType(SchemaNode schema) {
        if (!(schema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a primitive type");
        }
        // STRING can be BYTE_ARRAY with or without STRING logical type annotation
        if (primitive.type() != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' has physical type " + primitive.type()
                            + ", expected BYTE_ARRAY for STRING");
        }
    }

    private static void validateLogicalType(SchemaNode schema, Class<? extends LogicalType> expectedType) {
        if (!(schema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a primitive type");
        }
        LogicalType logicalType = primitive.logicalType();
        if (logicalType == null || !expectedType.isInstance(logicalType)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' has logical type "
                            + (logicalType == null ? "none" : logicalType.getClass().getSimpleName())
                            + ", expected " + expectedType.getSimpleName());
        }
    }

    private static void validateGroupType(SchemaNode schema, boolean expectList, boolean expectMap) {
        if (!(schema instanceof SchemaNode.GroupNode group)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a group type");
        }
        if (expectList && !group.isList()) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a list");
        }
        if (expectMap && !group.isMap()) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a map");
        }
        if (!expectList && !expectMap && (group.isList() || group.isMap())) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is a list or map, not a struct");
        }
    }

    private static Object convertToString(Object rawValue) {
        if (rawValue instanceof String) {
            return rawValue;
        }
        return new String((byte[]) rawValue, StandardCharsets.UTF_8);
    }

    private static <T> T convertLogicalType(Object rawValue, SchemaNode schema, Class<T> expectedClass) {
        // If already converted (e.g., by RecordAssembler for nested structures), return as-is
        if (expectedClass.isInstance(rawValue)) {
            return expectedClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
        Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
        return expectedClass.cast(converted);
    }
}
