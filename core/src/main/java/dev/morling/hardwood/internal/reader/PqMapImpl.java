/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqMap interface.
 */
public class PqMapImpl implements PqMap {

    private final List<Entry> entries;

    /**
     * Constructor for a map from assembled data.
     *
     * @param rawEntries list of Object[] where each array is [key, value]
     * @param mapSchema the MAP schema node
     */
    public PqMapImpl(List<?> rawEntries, SchemaNode.GroupNode mapSchema) {
        if (rawEntries == null || rawEntries.isEmpty()) {
            this.entries = Collections.emptyList();
            return;
        }

        // Get key/value schemas from MAP -> key_value -> (key, value)
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapSchema.children().get(0);
        SchemaNode keySchema = keyValueGroup.children().get(0);
        SchemaNode valueSchema = keyValueGroup.children().get(1);

        List<Entry> entryList = new ArrayList<>();
        for (Object rawEntry : rawEntries) {
            Object[] keyValuePair = (Object[]) rawEntry;
            entryList.add(new EntryImpl(keyValuePair[0], keyValuePair[1], keySchema, valueSchema));
        }
        this.entries = Collections.unmodifiableList(entryList);
    }

    @Override
    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Implementation of a map entry.
     */
    private static class EntryImpl implements Entry {

        private final Object key;
        private final Object value;
        private final SchemaNode keySchema;
        private final SchemaNode valueSchema;

        EntryImpl(Object key, Object value, SchemaNode keySchema, SchemaNode valueSchema) {
            this.key = key;
            this.value = value;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
        }

        @Override
        public <K> K getKey(PqType<K> type) {
            return ValueConverter.convert(key, type, keySchema);
        }

        @Override
        public <V> V getValue(PqType<V> type) {
            if (value == null) {
                return null;
            }
            return ValueConverter.convert(value, type, valueSchema);
        }

        @Override
        public boolean isValueNull() {
            return value == null;
        }
    }
}
