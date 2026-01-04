/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import dev.morling.hardwood.parquet.metadata.FileMetaData;
import dev.morling.hardwood.parquet.schema.MessageType;
import dev.morling.hardwood.parquet.thrift.ThriftCompactReader;

/**
 * Main reader for Parquet files.
 * Handles file structure validation and metadata parsing.
 */
public class ParquetFileReader implements AutoCloseable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private final RandomAccessFile file;
    private final FileMetaData fileMetaData;
    private final MessageType schema;

    private ParquetFileReader(RandomAccessFile file, FileMetaData fileMetaData, MessageType schema) {
        this.file = file;
        this.fileMetaData = fileMetaData;
        this.schema = schema;
    }

    public static ParquetFileReader open(Path path) throws IOException {
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
        try {
            // Validate file size
            long fileSize = file.length();
            if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
                throw new IOException("File too small to be a valid Parquet file");
            }

            // Read and validate magic number at start
            byte[] startMagic = new byte[MAGIC_SIZE];
            file.seek(0);
            file.readFully(startMagic);
            if (!java.util.Arrays.equals(startMagic, MAGIC)) {
                throw new IOException("Not a Parquet file (invalid magic number at start)");
            }

            // Read footer size and magic number at end
            file.seek(fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE);
            byte[] footerLengthBytes = new byte[FOOTER_LENGTH_SIZE];
            file.readFully(footerLengthBytes);
            int footerLength = ByteBuffer.wrap(footerLengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

            byte[] endMagic = new byte[MAGIC_SIZE];
            file.readFully(endMagic);
            if (!java.util.Arrays.equals(endMagic, MAGIC)) {
                throw new IOException("Not a Parquet file (invalid magic number at end)");
            }

            // Validate footer length
            long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
            if (footerStart < MAGIC_SIZE) {
                throw new IOException("Invalid footer length: " + footerLength);
            }

            // Read footer
            file.seek(footerStart);
            byte[] footerBytes = new byte[footerLength];
            file.readFully(footerBytes);

            // Parse file metadata
            ThriftCompactReader reader = new ThriftCompactReader(new ByteArrayInputStream(footerBytes));
            FileMetaData fileMetaData = FileMetaData.read(reader);

            // Reconstruct schema
            MessageType schema = MessageType.fromSchemaElements(fileMetaData.schema());

            return new ParquetFileReader(file, fileMetaData, schema);
        }
        catch (Exception e) {
            // Close file if there was an error during initialization
            try {
                file.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    public MessageType getSchema() {
        return schema;
    }

    public RandomAccessFile getFile() {
        return file;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
