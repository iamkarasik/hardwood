/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.event.FileMappingEvent;
import dev.hardwood.internal.reader.event.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * Memory-mapped file implementation of {@link ParquetFileReader}.
 *
 * <p>Maps the entire Parquet file into memory for efficient access to both
 * metadata and column data.</p>
 */
final class MemoryMappedParquetFileReader implements ParquetFileReader {

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer fileMapping;
    private final FileMetaData fileMetaData;
    private final HardwoodContextImpl context;
    private final boolean ownsContext;

    private MemoryMappedParquetFileReader(Path path, FileChannel channel, MappedByteBuffer fileMapping,
                                          FileMetaData fileMetaData, HardwoodContextImpl context, boolean ownsContext) {
        this.path = path;
        this.channel = channel;
        this.fileMapping = fileMapping;
        this.fileMetaData = fileMetaData;
        this.context = context;
        this.ownsContext = ownsContext;
    }

    static MemoryMappedParquetFileReader open(Path path, HardwoodContextImpl context,
                                              boolean ownsContext) throws IOException {
        FileOpenedEvent fileOpenedEvent = new FileOpenedEvent();
        fileOpenedEvent.begin();

        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            // Map the entire file once - used for both metadata and data reading
            long fileSize = channel.size();
            String fileName = path.getFileName().toString();

            FileMappingEvent event = new FileMappingEvent();
            event.begin();

            MappedByteBuffer fileMapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            event.file = fileName;
            event.offset = 0;
            event.size = fileSize;
            event.commit();

            // Read metadata from the mapping
            FileMetaData fileMetaData = ParquetMetadataReader.readMetadata(fileMapping, path);
            FileSchema fileSchema = FileSchema.fromSchemaElements(fileMetaData.schema());

            fileOpenedEvent.file = fileName;
            fileOpenedEvent.fileSize = fileSize;
            fileOpenedEvent.rowGroupCount = fileMetaData.rowGroups().size();
            fileOpenedEvent.columnCount = fileSchema.getColumnCount();
            fileOpenedEvent.commit();

            return new MemoryMappedParquetFileReader(path, channel, fileMapping, fileMetaData, context, ownsContext);
        }
        catch (Exception e) {
            // Close channel if there was an error during initialization
            try {
                channel.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    @Override
    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    @Override
    public FileSchema getFileSchema() {
        return FileSchema.fromSchemaElements(fileMetaData.schema());
    }

    @Override
    public ColumnReader createColumnReader(String columnName) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnName, schema, fileMapping, fileMetaData.rowGroups(), context);
    }

    @Override
    public ColumnReader createColumnReader(int columnIndex) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnIndex, schema, fileMapping, fileMetaData.rowGroups(), context);
    }

    @Override
    public RowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    @Override
    public RowReader createRowReader(ColumnProjection projection) {
        FileSchema schema = getFileSchema();
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);
        String fileName = path.getFileName().toString();
        return new SingleFileRowReader(schema, projectedSchema, fileMapping, fileMetaData.rowGroups(), context, fileName);
    }

    @Override
    public void close() throws IOException {
        // Only close context if we created it
        // When opened via Hardwood, the context is closed when Hardwood is closed
        if (ownsContext) {
            context.close();
        }

        // Close channel
        channel.close();
    }
}
