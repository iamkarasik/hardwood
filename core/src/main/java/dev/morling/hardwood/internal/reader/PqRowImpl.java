/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.List;

import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqRow interface.
 */
public class PqRowImpl implements PqRow {

    private final Object[] values;
    private final SchemaNode.GroupNode schema;

    /**
     * Constructor for top-level rows.
     */
    public PqRowImpl(Object[] values, FileSchema fileSchema) {
        this.values = values;
        this.schema = fileSchema.getRootNode();
    }

    /**
     * Constructor for nested struct rows.
     */
    public PqRowImpl(Object[] values, SchemaNode.GroupNode structSchema) {
        this.values = values;
        this.schema = structSchema;
    }

    @Override
    public <T> T getValue(PqType<T> type, int index) {
        Object rawValue = values[index];
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convert(rawValue, type, fieldSchema);
    }

    @Override
    public <T> T getValue(PqType<T> type, String name) {
        int index = getFieldIndex(name);
        Object rawValue = values[index];
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convert(rawValue, type, fieldSchema);
    }

    @Override
    public boolean isNull(int index) {
        return values[index] == null;
    }

    @Override
    public boolean isNull(String name) {
        return values[getFieldIndex(name)] == null;
    }

    @Override
    public int getFieldCount() {
        return schema.children().size();
    }

    @Override
    public String getFieldName(int index) {
        return schema.children().get(index).name();
    }

    private int getFieldIndex(String name) {
        List<SchemaNode> children = schema.children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }
}
