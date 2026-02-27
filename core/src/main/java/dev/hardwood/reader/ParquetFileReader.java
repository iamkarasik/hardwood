/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.file.Path;

import dev.hardwood.Hardwood;
import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

/**
 * Reader for individual Parquet files.
 *
 * <p>For single-file usage:</p>
 * <pre>{@code
 * try (ParquetFileReader reader = ParquetFileReader.open(path)) {
 *     RowReader rows = reader.createRowReader();
 *     // ...
 * }
 * }</pre>
 *
 * <p>For multi-file usage with shared thread pool, use {@link Hardwood}.</p>
 */
public interface ParquetFileReader extends AutoCloseable {

    /**
     * Open a Parquet file with a dedicated context.
     * The context is closed when this reader is closed.
     */
    static ParquetFileReader open(Path path) throws IOException {
        HardwoodContextImpl context = HardwoodContextImpl.create();
        return MemoryMappedParquetFileReader.open(path, context, true);
    }

    /**
     * Open a Parquet file with a shared context.
     * The context is NOT closed when this reader is closed.
     */
    static ParquetFileReader open(Path path, HardwoodContext context) throws IOException {
        return MemoryMappedParquetFileReader.open(path, (HardwoodContextImpl) context, false);
    }

    FileMetaData getFileMetaData();

    FileSchema getFileSchema();

    /**
     * Create a ColumnReader for a named column, spanning all row groups.
     */
    ColumnReader createColumnReader(String columnName);

    /**
     * Create a ColumnReader for a column by index, spanning all row groups.
     */
    ColumnReader createColumnReader(int columnIndex);

    /**
     * Create a RowReader that iterates over all rows in all row groups.
     */
    RowReader createRowReader();

    /**
     * Create a RowReader that iterates over selected columns in all row groups.
     *
     * @param projection specifies which columns to read
     * @return a RowReader for the selected columns
     */
    RowReader createRowReader(ColumnProjection projection);

    @Override
    void close() throws IOException;
}
