/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

import dev.morling.hardwood.internal.compression.Decompressor;
import dev.morling.hardwood.internal.compression.DecompressorFactory;
import dev.morling.hardwood.internal.encoding.ByteStreamSplitDecoder;
import dev.morling.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.morling.hardwood.internal.encoding.DeltaByteArrayDecoder;
import dev.morling.hardwood.internal.encoding.DeltaLengthByteArrayDecoder;
import dev.morling.hardwood.internal.encoding.PlainDecoder;
import dev.morling.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.morling.hardwood.internal.thrift.PageHeaderReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.metadata.DataPageHeader;
import dev.morling.hardwood.metadata.DataPageHeaderV2;
import dev.morling.hardwood.metadata.DictionaryPageHeader;
import dev.morling.hardwood.metadata.Encoding;
import dev.morling.hardwood.metadata.PageHeader;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reader for individual pages within a column chunk.
 * Uses a memory-mapped buffer (one per column chunk) for efficient, thread-safe access.
 */
public class PageReader {

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final MappedByteBuffer mappedBuffer;  // Mapped to this column chunk only
    private int currentPosition = 0;              // Position within the column chunk buffer
    private long valuesRead = 0;
    private Object[] dictionary = null;

    public PageReader(MappedByteBuffer mappedBuffer, ColumnMetaData columnMetaData, ColumnSchema column) {
        this.mappedBuffer = mappedBuffer;
        this.columnMetaData = columnMetaData;
        this.column = column;
    }

    /**
     * Read the next page. Returns null if no more pages.
     * Reads directly from memory-mapped buffer - thread-safe with no system calls.
     */
    public Page readPage() throws IOException {
        if (valuesRead >= columnMetaData.numValues()) {
            return null;
        }

        // Create a slice of the mapped buffer starting at current position for header parsing
        MappedByteBufferInputStream headerStream = new MappedByteBufferInputStream(mappedBuffer, currentPosition);
        ThriftCompactReader headerReader = new ThriftCompactReader(headerStream);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerStream.getBytesRead();

        // Read page data directly from the mapped buffer
        int compressedSize = pageHeader.compressedPageSize();
        int dataStart = currentPosition + headerSize;
        byte[] pageData = new byte[compressedSize];
        for (int i = 0; i < compressedSize; i++) {
            pageData[i] = mappedBuffer.get(dataStart + i);
        }

        // Update position for next page
        currentPosition += headerSize + compressedSize;

        // Handle different page types
        // Note: DATA_PAGE_V2 has different compression semantics - levels are uncompressed
        return switch (pageHeader.type()) {
            case DICTIONARY_PAGE -> {
                // Decompress entire page data for dictionary pages
                Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                // Read and store dictionary values
                parseDictionaryPage(pageHeader.dictionaryPageHeader(), uncompressedData);
                yield readPage(); // Read next page (the data page)
            }
            case DATA_PAGE -> {
                // Decompress entire page data for V1 data pages
                Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                DataPageHeader dataHeader = pageHeader.dataPageHeader();
                valuesRead += dataHeader.numValues();
                yield parseDataPage(dataHeader, uncompressedData);
            }
            case DATA_PAGE_V2 -> {
                // For V2, levels are stored uncompressed; only values may be compressed
                DataPageHeaderV2 dataHeaderV2 = pageHeader.dataPageHeaderV2();
                valuesRead += dataHeaderV2.numValues();
                yield parseDataPageV2(dataHeaderV2, pageData, pageHeader.uncompressedPageSize());
            }
            default -> throw new IOException("Unexpected page type: " + pageHeader.type());
        };
    }

    private Page parseDataPage(DataPageHeader header, byte[] data) throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        // In DATA_PAGE V1, order is: repetition levels, definition levels, values
        // Both rep and def levels have 4-byte length prefix

        // Read repetition levels
        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            int repLevelLength = readLittleEndianInt(dataStream);
            byte[] repLevelData = new byte[repLevelLength];
            dataStream.read(repLevelData);

            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        // Read definition levels
        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0) {
            int defLevelLength = readLittleEndianInt(dataStream);
            byte[] defLevelData = new byte[defLevelLength];
            dataStream.read(defLevelData);

            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        // Count non-null values
        int numNonNullValues = countNonNullValues(header.numValues(), definitionLevels);

        // Decode values and map to output array
        Object[] values = decodeAndMapValues(
                header.encoding(), dataStream, header.numValues(), numNonNullValues, definitionLevels);

        return new Page(header.numValues(), definitionLevels, repetitionLevels, values);
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, byte[] pageData, int uncompressedPageSize)
            throws IOException {
        // In DATA_PAGE_V2:
        // - Repetition levels are stored uncompressed
        // - Definition levels are stored uncompressed
        // - Only the values section may be compressed (controlled by is_compressed flag)

        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.length - valuesOffset;

        // Read repetition levels (uncompressed)
        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            byte[] repLevelData = new byte[repLevelLen];
            System.arraycopy(pageData, 0, repLevelData, 0, repLevelLen);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        // Read definition levels (uncompressed)
        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            byte[] defLevelData = new byte[defLevelLen];
            System.arraycopy(pageData, repLevelLen, defLevelData, 0, defLevelLen);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        // Decompress values section if needed
        byte[] valuesData;
        int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;

        // For DATA_PAGE_V2, decompress if is_compressed flag is true
        // Note: Snappy can expand data when compression isn't effective,
        // so compressed size may be >= uncompressed size
        // Special case: if compressedValuesLen is 0 (all nulls), skip decompression
        if (header.isCompressed() && compressedValuesLen > 0) {
            byte[] compressedValues = new byte[compressedValuesLen];
            System.arraycopy(pageData, valuesOffset, compressedValues, 0, compressedValuesLen);

            Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
            valuesData = decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        else {
            // Use data as-is (not compressed, or empty values section)
            valuesData = new byte[compressedValuesLen];
            System.arraycopy(pageData, valuesOffset, valuesData, 0, compressedValuesLen);
        }

        ByteArrayInputStream valuesStream = new ByteArrayInputStream(valuesData);

        // In V2, we have numNulls directly available
        int numNonNullValues = header.numValues() - header.numNulls();

        // Decode values and map to output array
        Object[] values = decodeAndMapValues(
                header.encoding(), valuesStream, header.numValues(), numNonNullValues, definitionLevels);

        return new Page(header.numValues(), definitionLevels, repetitionLevels, values);
    }

    /**
     * Decode levels using RLE/Bit-Packing Hybrid encoding.
     */
    private int[] decodeLevels(byte[] levelData, int numValues, int maxLevel) throws IOException {
        int[] levels = new int[numValues];
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                new ByteArrayInputStream(levelData), getBitWidth(maxLevel));
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /**
     * Count non-null values based on definition levels.
     */
    private int countNonNullValues(int numValues, int[] definitionLevels) {
        if (definitionLevels == null) {
            return numValues;
        }
        int maxDefLevel = column.maxDefinitionLevel();
        int count = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels[i] == maxDefLevel) {
                count++;
            }
        }
        return count;
    }

    /**
     * Decode values using the specified encoding and map them to the output array.
     *
     * @param encoding the encoding used for the values
     * @param dataStream the input stream containing encoded data
     * @param numValues total number of values (including nulls)
     * @param numNonNullValues number of non-null values to decode
     * @param definitionLevels definition levels for null handling (may be null)
     * @return array of decoded values with nulls in correct positions
     */
    private Object[] decodeAndMapValues(Encoding encoding, InputStream dataStream,
                                        int numValues, int numNonNullValues,
                                        int[] definitionLevels)
            throws IOException {
        Object[] values = new Object[numValues];

        switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(dataStream, column.type(), column.typeLength());
                decoder.readValues(values, definitionLevels, column.maxDefinitionLevel());
            }
            case RLE -> {
                // RLE encoding for boolean values uses bit-width of 1
                if (column.type() != PhysicalType.BOOLEAN) {
                    throw new UnsupportedOperationException(
                            "RLE encoding for non-boolean types not yet supported: " + column.type());
                }

                // Read 4-byte length prefix (little-endian)
                byte[] lengthBytes = new byte[4];
                dataStream.read(lengthBytes);
                int rleLength = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

                // Read the RLE-encoded data
                byte[] rleData = new byte[rleLength];
                dataStream.read(rleData);

                RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                        new ByteArrayInputStream(rleData), 1);
                decoder.readBooleans(values, definitionLevels, column.maxDefinitionLevel());
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new IOException("Dictionary page not found for " + encoding + " encoding");
                }

                // RLE_DICTIONARY encoding always starts with 1-byte bit-width prefix
                int bitWidth = dataStream.read();
                if (bitWidth < 0) {
                    throw new IOException("Failed to read bit width for dictionary indices");
                }

                byte[] indicesData = dataStream.readAllBytes();
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(
                        new ByteArrayInputStream(indicesData), bitWidth);
                indexDecoder.readDictionaryValues(values, dictionary, definitionLevels, column.maxDefinitionLevel());
            }
            case DELTA_BINARY_PACKED -> {
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(dataStream, column.type());
                decoder.readValues(values, definitionLevels, column.maxDefinitionLevel());
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(dataStream);
                decoder.initialize(numNonNullValues);
                decoder.readValues(values, definitionLevels, column.maxDefinitionLevel());
            }
            case DELTA_BYTE_ARRAY -> {
                DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(dataStream);
                decoder.initialize(numNonNullValues);
                decoder.readValues(values, definitionLevels, column.maxDefinitionLevel());
            }
            case BYTE_STREAM_SPLIT -> {
                // BYTE_STREAM_SPLIT needs all data upfront to compute stream offsets
                byte[] allData = dataStream.readAllBytes();
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        allData, numNonNullValues, column.type(), column.typeLength());
                decoder.readValues(values, definitionLevels, column.maxDefinitionLevel());
            }
            default -> throw new UnsupportedOperationException("Encoding not yet supported: " + encoding);
        }

        return values;
    }

    /**
     * Read a 4-byte little-endian integer from the stream.
     */
    private int readLittleEndianInt(InputStream stream) throws IOException {
        byte[] bytes = new byte[4];
        stream.read(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private void parseDictionaryPage(DictionaryPageHeader header, byte[] data)
            throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        // Dictionary values are encoded with PLAIN or PLAIN_DICTIONARY encoding
        if (header.encoding() != Encoding.PLAIN && header.encoding() != Encoding.PLAIN_DICTIONARY) {
            throw new UnsupportedOperationException(
                    "Dictionary encoding not yet supported: " + header.encoding());
        }

        // Read all dictionary values (both PLAIN and PLAIN_DICTIONARY use plain encoding for dictionary)
        dictionary = new Object[header.numValues()];
        PlainDecoder decoder = new PlainDecoder(dataStream, column.type(), column.typeLength());
        decoder.readValues(dictionary, 0, header.numValues());
    }

    private int getBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }

    /**
     * InputStream that reads from a MappedByteBuffer at a given offset.
     * Tracks bytes read for determining header size.
     */
    private static class MappedByteBufferInputStream extends InputStream {
        private final MappedByteBuffer buffer;
        private final int startOffset;
        private int pos;

        public MappedByteBufferInputStream(MappedByteBuffer buffer, int startOffset) {
            this.buffer = buffer;
            this.startOffset = startOffset;
            this.pos = startOffset;
        }

        @Override
        public int read() {
            if (pos >= buffer.limit()) {
                return -1;
            }
            return buffer.get(pos++) & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= buffer.limit()) {
                return -1;
            }
            int available = Math.min(len, buffer.limit() - pos);
            for (int i = 0; i < available; i++) {
                b[off + i] = buffer.get(pos++);
            }
            return available;
        }

        public int getBytesRead() {
            return pos - startOffset;
        }
    }

    public static record Page(int numValues, int[] definitionLevels, int[] repetitionLevels, Object[] values) {
    }

    public static record TypedPage(int numValues, int[] definitionLevels, int[] repetitionLevels,
                                   TypedColumnData columnData) {
    }

    /**
     * Read the next page with typed primitive storage.
     * Returns null if no more pages.
     */
    public TypedPage readTypedPage() throws IOException {
        if (valuesRead >= columnMetaData.numValues()) {
            return null;
        }

        // Create a slice of the mapped buffer starting at current position for header parsing
        MappedByteBufferInputStream headerStream = new MappedByteBufferInputStream(mappedBuffer, currentPosition);
        ThriftCompactReader headerReader = new ThriftCompactReader(headerStream);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerStream.getBytesRead();

        // Read page data directly from the mapped buffer
        int compressedSize = pageHeader.compressedPageSize();
        int dataStart = currentPosition + headerSize;
        byte[] pageData = new byte[compressedSize];
        for (int i = 0; i < compressedSize; i++) {
            pageData[i] = mappedBuffer.get(dataStart + i);
        }

        // Update position for next page
        currentPosition += headerSize + compressedSize;

        return switch (pageHeader.type()) {
            case DICTIONARY_PAGE -> {
                Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                parseDictionaryPage(pageHeader.dictionaryPageHeader(), uncompressedData);
                yield readTypedPage();
            }
            case DATA_PAGE -> {
                Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                DataPageHeader dataHeader = pageHeader.dataPageHeader();
                valuesRead += dataHeader.numValues();
                yield parseTypedDataPage(dataHeader, uncompressedData);
            }
            case DATA_PAGE_V2 -> {
                DataPageHeaderV2 dataHeaderV2 = pageHeader.dataPageHeaderV2();
                valuesRead += dataHeaderV2.numValues();
                yield parseTypedDataPageV2(dataHeaderV2, pageData, pageHeader.uncompressedPageSize());
            }
            default -> throw new IOException("Unexpected page type: " + pageHeader.type());
        };
    }

    private TypedPage parseTypedDataPage(DataPageHeader header, byte[] data) throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            int repLevelLength = readLittleEndianInt(dataStream);
            byte[] repLevelData = new byte[repLevelLength];
            dataStream.read(repLevelData);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0) {
            int defLevelLength = readLittleEndianInt(dataStream);
            byte[] defLevelData = new byte[defLevelLength];
            dataStream.read(defLevelData);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        int numNonNullValues = countNonNullValues(header.numValues(), definitionLevels);

        TypedColumnData columnData = decodeTypedValues(
                header.encoding(), dataStream, header.numValues(), numNonNullValues, definitionLevels);

        return new TypedPage(header.numValues(), definitionLevels, repetitionLevels, columnData);
    }

    private TypedPage parseTypedDataPageV2(DataPageHeaderV2 header, byte[] pageData, int uncompressedPageSize)
            throws IOException {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.length - valuesOffset;

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            byte[] repLevelData = new byte[repLevelLen];
            System.arraycopy(pageData, 0, repLevelData, 0, repLevelLen);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            byte[] defLevelData = new byte[defLevelLen];
            System.arraycopy(pageData, repLevelLen, defLevelData, 0, defLevelLen);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        byte[] valuesData;
        int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;

        if (header.isCompressed() && compressedValuesLen > 0) {
            byte[] compressedValues = new byte[compressedValuesLen];
            System.arraycopy(pageData, valuesOffset, compressedValues, 0, compressedValuesLen);
            Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
            valuesData = decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        else {
            valuesData = new byte[compressedValuesLen];
            System.arraycopy(pageData, valuesOffset, valuesData, 0, compressedValuesLen);
        }

        ByteArrayInputStream valuesStream = new ByteArrayInputStream(valuesData);
        int numNonNullValues = header.numValues() - header.numNulls();

        TypedColumnData columnData = decodeTypedValues(
                header.encoding(), valuesStream, header.numValues(), numNonNullValues, definitionLevels);

        return new TypedPage(header.numValues(), definitionLevels, repetitionLevels, columnData);
    }

    /**
     * Decode values into TypedColumnData using primitive arrays where possible.
     */
    private TypedColumnData decodeTypedValues(Encoding encoding, InputStream dataStream,
                                              int numValues, int numNonNullValues,
                                              int[] definitionLevels) throws IOException {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

        // Try to decode into primitive arrays for supported type/encoding combinations
        switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(dataStream, type, column.typeLength());
                return switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.LongColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.DoubleColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.IntColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    default -> {
                        Object[] values = new Object[numValues];
                        decoder.readValues(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.ObjectColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            case DELTA_BINARY_PACKED -> {
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(dataStream, type);
                return switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.LongColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.IntColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    default -> {
                        Object[] values = new Object[numValues];
                        decoder.readValues(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.ObjectColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            case BYTE_STREAM_SPLIT -> {
                byte[] allData = dataStream.readAllBytes();
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        allData, numNonNullValues, type, column.typeLength());
                return switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.LongColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.DoubleColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.IntColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    default -> {
                        Object[] values = new Object[numValues];
                        decoder.readValues(values, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.ObjectColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new IOException("Dictionary page not found for " + encoding + " encoding");
                }
                int bitWidth = dataStream.read();
                if (bitWidth < 0) {
                    throw new IOException("Failed to read bit width for dictionary indices");
                }
                byte[] indicesData = dataStream.readAllBytes();
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(
                        new ByteArrayInputStream(indicesData), bitWidth);

                // Check if we can use primitive dictionary lookup
                return switch (type) {
                    case INT64 -> {
                        long[] primitiveDictionary = new long[dictionary.length];
                        for (int i = 0; i < dictionary.length; i++) {
                            primitiveDictionary[i] = (Long) dictionary[i];
                        }
                        long[] values = new long[numValues];
                        indexDecoder.readDictionaryLongs(values, primitiveDictionary, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.LongColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] primitiveDictionary = new double[dictionary.length];
                        for (int i = 0; i < dictionary.length; i++) {
                            primitiveDictionary[i] = (Double) dictionary[i];
                        }
                        double[] values = new double[numValues];
                        indexDecoder.readDictionaryDoubles(values, primitiveDictionary, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.DoubleColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] primitiveDictionary = new int[dictionary.length];
                        for (int i = 0; i < dictionary.length; i++) {
                            primitiveDictionary[i] = (Integer) dictionary[i];
                        }
                        int[] values = new int[numValues];
                        indexDecoder.readDictionaryInts(values, primitiveDictionary, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.IntColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                    default -> {
                        Object[] values = new Object[numValues];
                        indexDecoder.readDictionaryValues(values, dictionary, definitionLevels, maxDefLevel);
                        yield new TypedColumnData.ObjectColumn(values, definitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            default -> {
                // Fall back to Object[] for other encodings
                Object[] values = decodeAndMapValues(encoding, dataStream, numValues, numNonNullValues, definitionLevels);
                return new TypedColumnData.ObjectColumn(values, definitionLevels, maxDefLevel, numValues);
            }
        }
    }
}
