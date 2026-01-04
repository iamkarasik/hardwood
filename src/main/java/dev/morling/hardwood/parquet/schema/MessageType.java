/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.parquet.schema;

import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.parquet.metadata.SchemaElement;

/**
 * Root schema container representing the complete Parquet schema.
 * For Milestone 1, supports flat schemas only.
 */
public class MessageType {

    private final String name;
    private final List<Column> columns;

    private MessageType(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public Column getColumn(String name) {
        for (Column column : columns) {
            if (column.name().equals(name)) {
                return column;
            }
        }
        throw new IllegalArgumentException("Column not found: " + name);
    }

    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Reconstruct schema from Thrift SchemaElement list.
     */
    public static MessageType fromSchemaElements(List<SchemaElement> elements) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Schema elements list is empty");
        }

        SchemaElement root = elements.get(0);
        if (root.isPrimitive()) {
            throw new IllegalArgumentException("Root schema element must be a group");
        }

        List<Column> columns = new ArrayList<>();
        int numChildren = root.numChildren() != null ? root.numChildren() : 0;

        for (int i = 0; i < numChildren; i++) {
            SchemaElement element = elements.get(i + 1);
            if (!element.isPrimitive()) {
                // For Milestone 1, skip nested structures
                continue;
            }

            columns.add(new Column(
                    element.name(),
                    element.type(),
                    element.repetitionType(),
                    element.typeLength(),
                    i));
        }

        return new MessageType(root.name(), columns);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("message ").append(name).append(" {\n");
        for (Column column : columns) {
            sb.append("  ").append(column).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
