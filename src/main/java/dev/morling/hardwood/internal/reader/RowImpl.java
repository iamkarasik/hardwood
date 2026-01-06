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
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.row.Row;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Implementation of Row interface backed by an Object array.
 */
public class RowImpl implements Row {

    private final Object[] values;
    private final FileSchema schema;

    public RowImpl(Object[] values, FileSchema schema) {
        this.values = values;
        this.schema = schema;
    }

    @Override
    public boolean getBoolean(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Boolean) value;
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(getColumnIndex(name));
    }

    @Override
    public int getInt(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Integer) value;
    }

    @Override
    public int getInt(String name) {
        return getInt(getColumnIndex(name));
    }

    @Override
    public long getLong(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Long) value;
    }

    @Override
    public long getLong(String name) {
        return getLong(getColumnIndex(name));
    }

    @Override
    public float getFloat(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Float) value;
    }

    @Override
    public float getFloat(String name) {
        return getFloat(getColumnIndex(name));
    }

    @Override
    public double getDouble(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Double) value;
    }

    @Override
    public double getDouble(String name) {
        return getDouble(getColumnIndex(name));
    }

    @Override
    public byte[] getByteArray(int position) {
        return (byte[]) values[position];
    }

    @Override
    public byte[] getByteArray(String name) {
        return getByteArray(getColumnIndex(name));
    }

    @Override
    public String getString(int position) {
        byte[] bytes = getByteArray(position);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public String getString(String name) {
        return getString(getColumnIndex(name));
    }

    @Override
    public LocalDate getDate(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof LocalDate)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a DATE type");
        }
        return (LocalDate) converted;
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(getColumnIndex(name));
    }

    @Override
    public LocalTime getTime(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof LocalTime)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a TIME type");
        }
        return (LocalTime) converted;
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(getColumnIndex(name));
    }

    @Override
    public Instant getTimestamp(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof Instant)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a TIMESTAMP type");
        }
        return (Instant) converted;
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(getColumnIndex(name));
    }

    @Override
    public BigDecimal getDecimal(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof BigDecimal)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a DECIMAL type");
        }
        return (BigDecimal) converted;
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(getColumnIndex(name));
    }

    @Override
    public UUID getUuid(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof UUID)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a UUID type");
        }
        return (UUID) converted;
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(getColumnIndex(name));
    }

    @Override
    public Object getObject(int position) {
        Object value = values[position];
        if (value == null) {
            return null;
        }

        ColumnSchema column = schema.getColumn(position);
        return LogicalTypeConverter.convert(value, column.type(), column.logicalType());
    }

    @Override
    public Object getObject(String name) {
        return getObject(getColumnIndex(name));
    }

    @Override
    public boolean isNull(int position) {
        return values[position] == null;
    }

    @Override
    public boolean isNull(String name) {
        return isNull(getColumnIndex(name));
    }

    @Override
    public int getColumnCount() {
        return schema.getColumnCount();
    }

    @Override
    public String getColumnName(int position) {
        return schema.getColumn(position).name();
    }

    private int getColumnIndex(String name) {
        return schema.getColumn(name).columnIndex();
    }
}
