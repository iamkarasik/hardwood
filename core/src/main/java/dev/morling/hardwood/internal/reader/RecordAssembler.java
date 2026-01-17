/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.List;

import dev.morling.hardwood.schema.FieldPath;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Independent column processing record assembler for Parquet data.
 *
 * <p>Each column is processed completely independently - the repetition levels alone
 * determine the exact "coordinates" (indices) where each value belongs in the nested
 * structure. Sibling columns are guaranteed to have parallel structure, so processing
 * them separately and merging by index is guaranteed to align correctly.</p>
 *
 * <h2>Algorithm</h2>
 * <pre>
 * For each column:
 *   indices = [0, 0, ...]  // One index per repetition level
 *   For each value in column (for current record):
 *     updateIndices(r)  // Compute position from rep level
 *     insertAtPath(record, path, indices, d, value)
 *
 * updateIndices(r):
 *   // Reset everything deeper than r to 0
 *   for i = r+1 to maxRepLevel: indices[i] = 0
 *   // Increment at level r (except r=0 which starts new record)
 *   if r > 0: indices[r]++
 * </pre>
 *
 * <h2>Example: [[1, 2], [3], [4, 5, 6]]</h2>
 * <pre>
 * Value | r | indices after  | Position
 * ------|---|----------------|----------
 *   1   | 0 | [0, 0]         | [0][0]
 *   2   | 2 | [0, 1]         | [0][1]
 *   3   | 1 | [1, 0]         | [1][0]
 *   4   | 1 | [2, 0]         | [2][0]
 *   5   | 2 | [2, 1]         | [2][1]
 *   6   | 2 | [2, 2]         | [2][2]
 * </pre>
 */
public class RecordAssembler {

    private final FileSchema schema;

    public RecordAssembler(FileSchema schema) {
        this.schema = schema;
    }

    /**
     * Assemble the current record from all column batches.
     * Each batch must have had {@link ColumnBatch#nextRecord()} called before this method.
     */
    public MutableStruct assembleRow(List<ColumnBatch> batches) {
        int rootSize = schema.getRootNode().children().size();
        MutableStruct record = new MutableStruct(rootSize);

        for (ColumnBatch batch : batches) {
            int colIndex = batch.getColumn().columnIndex();
            processColumn(batch, schema.getFieldPaths().get(colIndex), record);
        }

        return record;
    }

    /**
     * Process a single column's current record, inserting values into the record.
     */
    private void processColumn(ColumnBatch batch, FieldPath path, MutableStruct record) {
        int maxRepLevel = batch.getColumn().maxRepetitionLevel();
        int[] indices = new int[maxRepLevel + 1];

        while (batch.hasValue()) {
            int r = batch.repetitionLevel();
            int d = batch.definitionLevel();
            Object value = batch.value();
            batch.advance();

            updateIndices(indices, r);
            insertAtPath(record, path, indices, d, value);
        }
    }

    /**
     * Update indices based on repetition level.
     *
     * <p>The repetition level r means "repeating at level r":</p>
     * <ul>
     *   <li>Reset everything deeper than r to 0 (new element starts fresh)</li>
     *   <li>Increment at level r (except r=0 which starts a new record)</li>
     * </ul>
     */
    private void updateIndices(int[] indices, int r) {
        // Reset all levels deeper than r
        for (int i = r + 1; i < indices.length; i++) {
            indices[i] = 0;
        }

        // Increment at level r (except for r=0 which starts a new record)
        if (r > 0) {
            indices[r]++;
        }
    }

    /**
     * Insert a value into the record at the position determined by the path and indices.
     */
    private void insertAtPath(MutableStruct record, FieldPath path, int[] indices,
                              int defLevel, Object value) {
        MutableContainer current = record;
        int indexPtr = 0;

        FieldPath.PathStep[] steps = path.steps();
        for (int level = 0; level < steps.length; level++) {
            FieldPath.PathStep step = steps[level];

            if (step.definitionLevel() > defLevel) {
                return;
            }

            if (step.isRepeated()) {
                int idx = indices[++indexPtr];

                if (step.isContainer()) {
                    current = current.getOrCreateChild(idx, step);
                }
                else {
                    // Primitive element - set value and return
                    if (defLevel == path.maxDefLevel()) {
                        current.setChild(idx, value);
                    }
                    return;
                }
            }
            else if (step.isContainer()) {
                // Skip nested container if previous element step already created it
                if ((step.isList() || step.isMap()) && level > 0) {
                    FieldPath.PathStep prev = steps[level - 1];
                    if (prev.isRepeated() && (prev.isList() || prev.isMap())) {
                        continue;
                    }
                }
                current = current.getOrCreateChild(step.fieldIndex(), step);
            }
        }

        if (defLevel == path.maxDefLevel()) {
            current.setChild(path.leafFieldIndex(), value);
        }
    }
}
