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
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Implementation of PqRow that reads directly from typed prefetched column data at a given row index.
 * <p>
 * This implementation is optimized for flat schemas (no nested/repeated fields) where
 * row N maps directly to index N in each column's prefetched data. For flat schemas, this avoids
 * the overhead of record assembly via {@link RecordAssembler} and eliminates boxing for
 * primitive types by reading directly from typed primitive arrays.
 * </p>
 * <p>
 * Nested field accessors ({@link #getRow}, {@link #getList}, {@link #getMap}) throw
 * {@link UnsupportedOperationException} as this implementation is only used for flat schemas.
 * </p>
 */
public class ColumnarPqRowImpl implements PqRow {

    private final List<TypedColumnData> columns;
    private final int rowIndex;
    private final FileSchema schema;

    /**
     * Create a columnar PqRow backed by typed prefetched column data.
     *
     * @param columns  the typed prefetched column data (one per column)
     * @param rowIndex the row index within the current batch
     * @param schema   the file schema for column lookup
     */
    public ColumnarPqRowImpl(List<TypedColumnData> columns, int rowIndex, FileSchema schema) {
        this.columns = columns;
        this.rowIndex = rowIndex;
        this.schema = schema;
    }

    // ==================== Primitive Types ====================

    @Override
    public int getInt(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT32);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.IntColumn) data).get(rowIndex);
    }

    @Override
    public long getLong(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT64);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.LongColumn) data).get(rowIndex);
    }

    @Override
    public float getFloat(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.FLOAT);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.FloatColumn) data).get(rowIndex);
    }

    @Override
    public double getDouble(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.DOUBLE);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.DoubleColumn) data).get(rowIndex);
    }

    @Override
    public boolean getBoolean(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.BOOLEAN);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.BooleanColumn) data).get(rowIndex);
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        return new String(getByteArrayValue(data), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        return getByteArrayValue(data);
    }

    @Override
    public LocalDate getDate(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        int rawValue = ((TypedColumnData.IntColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    @Override
    public LocalTime getTime(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        Object rawValue;
        if (col.type() == PhysicalType.INT32) {
            rawValue = ((TypedColumnData.IntColumn) data).get(rowIndex);
        }
        else {
            rawValue = ((TypedColumnData.LongColumn) data).get(rowIndex);
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(), (LogicalType.TimeType) col.logicalType());
    }

    @Override
    public Instant getTimestamp(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        long rawValue = ((TypedColumnData.LongColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(), (LogicalType.TimestampType) col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        Object rawValue = switch (col.type()) {
            case INT32 -> ((TypedColumnData.IntColumn) data).get(rowIndex);
            case INT64 -> ((TypedColumnData.LongColumn) data).get(rowIndex);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> getByteArrayValue(data);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(), (LogicalType.DecimalType) col.logicalType());
    }

    @Override
    public UUID getUuid(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        return LogicalTypeConverter.convertToUuid(getByteArrayValue(data), col.type());
    }

    // ==================== Nested Types (not supported for flat schemas) ====================

    @Override
    public PqRow getRow(String name) {
        throw new UnsupportedOperationException(
                "Nested struct access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqIntList getListOfInts(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqList getList(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqMap getMap(String name) {
        throw new UnsupportedOperationException(
                "Map access not supported in columnar mode. Schema is expected to be flat.");
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        if (data.isNull(rowIndex)) {
            return null;
        }
        return getBoxedValue(data);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        ColumnSchema col = schema.getColumn(name);
        TypedColumnData data = columns.get(col.columnIndex());
        return data.isNull(rowIndex);
    }

    @Override
    public int getFieldCount() {
        return schema.getColumnCount();
    }

    @Override
    public String getFieldName(int index) {
        return schema.getColumn(index).name();
    }

    // ==================== Internal Helpers ====================

    /**
     * Get a byte array value from typed data.
     */
    private byte[] getByteArrayValue(TypedColumnData data) {
        return ((TypedColumnData.ByteArrayColumn) data).get(rowIndex);
    }

    /**
     * Get a value from typed data, boxing primitives if needed.
     * Used for logical type conversions that require the raw physical value.
     */
    private Object getBoxedValue(TypedColumnData data) {
        return switch (data) {
            case TypedColumnData.IntColumn intCol -> intCol.get(rowIndex);
            case TypedColumnData.LongColumn longCol -> longCol.get(rowIndex);
            case TypedColumnData.FloatColumn floatCol -> floatCol.get(rowIndex);
            case TypedColumnData.DoubleColumn doubleCol -> doubleCol.get(rowIndex);
            case TypedColumnData.BooleanColumn boolCol -> boolCol.get(rowIndex);
            case TypedColumnData.ByteArrayColumn byteCol -> byteCol.get(rowIndex);
        };
    }

    /**
     * Validate that the column has the expected physical type.
     */
    private void validatePhysicalType(ColumnSchema col, PhysicalType... expectedTypes) {
        for (PhysicalType expected : expectedTypes) {
            if (col.type() == expected) {
                return;
            }
        }
        String expectedStr = expectedTypes.length == 1
                ? expectedTypes[0].toString()
                : java.util.Arrays.toString(expectedTypes);
        throw new IllegalArgumentException(
                "Field '" + col.name() + "' has physical type " + col.type() + ", expected " + expectedStr);
    }
}
