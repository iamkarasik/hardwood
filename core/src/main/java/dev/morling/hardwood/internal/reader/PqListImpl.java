/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.Iterator;

import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqList interface.
 */
public class PqListImpl implements PqList {

    private final MutableList elements;
    private final SchemaNode elementSchema;

    public PqListImpl(MutableList elements, SchemaNode.GroupNode listSchema) {
        this.elements = elements;
        this.elementSchema = listSchema.getListElement();
    }

    @Override
    public <T> Iterable<T> getValues(PqType<T> elementType) {
        if (elementSchema == null) {
            throw new IllegalStateException("List has no element schema");
        }
        return () -> new ConvertingIterator<>(elements.elements().iterator(), elementType);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean isEmpty() {
        return elements.size() == 0;
    }

    private class ConvertingIterator<T> implements Iterator<T> {
        private final Iterator<?> delegate;
        private final PqType<T> type;

        ConvertingIterator(Iterator<?> delegate, PqType<T> type) {
            this.delegate = delegate;
            this.type = type;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            Object rawValue = delegate.next();
            return ValueConverter.convert(rawValue, type, elementSchema);
        }
    }
}
