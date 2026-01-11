/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.example.data.simple;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;

/**
 * SimpleGroup implementation that wraps Hardwood's PqRow.
 * <p>
 * This class provides parquet-java compatible Group access by delegating
 * to Hardwood's PqRow API.
 * </p>
 */
public class SimpleGroup implements Group {

    private final PqRow row;
    private final GroupType schema;

    /**
     * Create a SimpleGroup wrapping a PqRow.
     *
     * @param row the Hardwood PqRow
     * @param schema the GroupType schema
     */
    public SimpleGroup(PqRow row, GroupType schema) {
        this.row = row;
        this.schema = schema;
    }

    @Override
    public GroupType getType() {
        return schema;
    }

    @Override
    public int getFieldRepetitionCount(int fieldIndex) {
        Type fieldType = schema.getType(fieldIndex);
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            // For repeated fields, get the list and return size
            if (row.isNull(fieldIndex)) {
                return 0;
            }
            PqList list = row.getValue(PqType.LIST, fieldIndex);
            return list != null ? list.size() : 0;
        }
        // For non-repeated fields, return 1 if present, 0 if null
        return row.isNull(fieldIndex) ? 0 : 1;
    }

    @Override
    public int getFieldRepetitionCount(String field) {
        return getFieldRepetitionCount(schema.getFieldIndex(field));
    }

    // ---- String getters ----

    @Override
    public String getString(int fieldIndex, int index) {
        return getValueAtIndex(fieldIndex, index, PqType.STRING);
    }

    @Override
    public String getString(String field, int index) {
        return getString(schema.getFieldIndex(field), index);
    }

    // ---- Integer getters ----

    @Override
    public int getInteger(int fieldIndex, int index) {
        Integer value = getValueAtIndex(fieldIndex, index, PqType.INT32);
        return value != null ? value : 0;
    }

    @Override
    public int getInteger(String field, int index) {
        return getInteger(schema.getFieldIndex(field), index);
    }

    // ---- Long getters ----

    @Override
    public long getLong(int fieldIndex, int index) {
        Long value = getValueAtIndex(fieldIndex, index, PqType.INT64);
        return value != null ? value : 0L;
    }

    @Override
    public long getLong(String field, int index) {
        return getLong(schema.getFieldIndex(field), index);
    }

    // ---- Double getters ----

    @Override
    public double getDouble(int fieldIndex, int index) {
        Double value = getValueAtIndex(fieldIndex, index, PqType.DOUBLE);
        return value != null ? value : 0.0;
    }

    @Override
    public double getDouble(String field, int index) {
        return getDouble(schema.getFieldIndex(field), index);
    }

    // ---- Float getters ----

    @Override
    public float getFloat(int fieldIndex, int index) {
        Float value = getValueAtIndex(fieldIndex, index, PqType.FLOAT);
        return value != null ? value : 0.0f;
    }

    @Override
    public float getFloat(String field, int index) {
        return getFloat(schema.getFieldIndex(field), index);
    }

    // ---- Boolean getters ----

    @Override
    public boolean getBoolean(int fieldIndex, int index) {
        Boolean value = getValueAtIndex(fieldIndex, index, PqType.BOOLEAN);
        return value != null ? value : false;
    }

    @Override
    public boolean getBoolean(String field, int index) {
        return getBoolean(schema.getFieldIndex(field), index);
    }

    // ---- Binary getters ----

    @Override
    public Binary getBinary(int fieldIndex, int index) {
        byte[] bytes = getValueAtIndex(fieldIndex, index, PqType.BINARY);
        return bytes != null ? Binary.fromConstantByteArray(bytes) : null;
    }

    @Override
    public Binary getBinary(String field, int index) {
        return getBinary(schema.getFieldIndex(field), index);
    }

    // ---- Group getters ----

    @Override
    public Group getGroup(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        GroupType nestedType = fieldType.asGroupType();

        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            // Repeated group - get from list
            PqList list = row.getValue(PqType.LIST, fieldIndex);
            if (list == null || index >= list.size()) {
                return null;
            }
            int i = 0;
            for (PqRow nestedRow : list.getValues(PqType.ROW)) {
                if (i == index) {
                    return new SimpleGroup(nestedRow, nestedType);
                }
                i++;
            }
            return null;
        }
        else {
            // Single group
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            PqRow nestedRow = row.getValue(PqType.ROW, fieldIndex);
            return nestedRow != null ? new SimpleGroup(nestedRow, nestedType) : null;
        }
    }

    @Override
    public Group getGroup(String field, int index) {
        return getGroup(schema.getFieldIndex(field), index);
    }

    // ---- Helper methods ----

    /**
     * Get a value at a specific index, handling repeated fields.
     */
    private <T> T getValueAtIndex(int fieldIndex, int index, PqType<T> pqType) {
        Type fieldType = schema.getType(fieldIndex);

        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            // Repeated field - get from list
            PqList list = row.getValue(PqType.LIST, fieldIndex);
            if (list == null || index >= list.size()) {
                return null;
            }
            int i = 0;
            for (T value : list.getValues(pqType)) {
                if (i == index) {
                    return value;
                }
                i++;
            }
            return null;
        }
        else {
            // Non-repeated field
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            if (row.isNull(fieldIndex)) {
                return null;
            }
            return row.getValue(pqType, fieldIndex);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(schema.getName()).append(" {");
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (i > 0)
                sb.append(", ");
            Type fieldType = schema.getType(i);
            sb.append(fieldType.getName()).append("=");
            int count = getFieldRepetitionCount(i);
            if (count == 0) {
                sb.append("null");
            }
            else if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
                sb.append("[").append(count).append(" values]");
            }
            else {
                sb.append("...");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
