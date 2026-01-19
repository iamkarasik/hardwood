/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.morling.hardwood.metadata.PhysicalType;

/**
 * Decoder for BYTE_STREAM_SPLIT encoding.
 *
 * BYTE_STREAM_SPLIT scatters bytes of values across K streams (where K is the byte width).
 * For N values of K bytes each:
 * - Stream 0 contains byte 0 of all N values
 * - Stream 1 contains byte 1 of all N values
 * - etc.
 *
 * The encoded data is the concatenation of all streams: [stream0][stream1]...[streamK-1]
 *
 * To decode value i, gather byte[i] from each stream and reassemble.
 */
public class ByteStreamSplitDecoder implements ValueDecoder {

    private final byte[] data;
    private final int numValues;
    private final int byteWidth;
    private final PhysicalType type;
    private int currentIndex = 0;

    public ByteStreamSplitDecoder(byte[] data, int numValues, PhysicalType type, Integer typeLength) {
        this.data = data;
        this.numValues = numValues;
        this.type = type;
        this.byteWidth = getByteWidth(type, typeLength);

        // Validate data length
        int expectedLength = numValues * byteWidth;
        if (data.length != expectedLength) {
            throw new IllegalArgumentException(
                    "Data length mismatch: expected " + expectedLength + " bytes for " +
                            numValues + " values of " + byteWidth + " bytes, got " + data.length);
        }
    }

    private static int getByteWidth(PhysicalType type, Integer typeLength) {
        return switch (type) {
            case FLOAT, INT32 -> 4;
            case DOUBLE, INT64 -> 8;
            case FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength == null) {
                    throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY requires typeLength");
                }
                yield typeLength;
            }
            default -> throw new UnsupportedOperationException(
                    "BYTE_STREAM_SPLIT not supported for type: " + type);
        };
    }

    /**
     * Read the next value.
     */
    public Object readValue() throws IOException {
        if (currentIndex >= numValues) {
            throw new IOException("No more values to read");
        }

        byte[] valueBytes = new byte[byteWidth];

        // Gather bytes from each stream
        // Stream k starts at offset (k * numValues) and contains byte k of all values
        for (int k = 0; k < byteWidth; k++) {
            int streamOffset = k * numValues;
            valueBytes[k] = data[streamOffset + currentIndex];
        }

        currentIndex++;

        return convertToValue(valueBytes);
    }

    private Object convertToValue(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case FLOAT -> buffer.getFloat();
            case DOUBLE -> buffer.getDouble();
            case INT32 -> buffer.getInt();
            case INT64 -> buffer.getLong();
            case FIXED_LEN_BYTE_ARRAY -> bytes;
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }

    @Override
    public void readValues(Object[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = readValue();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = readValue();
                }
            }
        }
    }

    /**
     * Read DOUBLE values directly into a primitive double array.
     */
    @Override
    public void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getDouble();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getDouble();
                }
            }
        }
    }

    /**
     * Read INT64 values directly into a primitive long array.
     */
    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getLong();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getLong();
                }
            }
        }
    }

    /**
     * Read INT32 values directly into a primitive int array.
     */
    @Override
    public void readInts(int[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[4];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getInt();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getInt();
                }
            }
        }
    }

    /**
     * Gather bytes for the current value from byte streams and advance the index.
     */
    private void gatherBytes(byte[] valueBytes) throws IOException {
        if (currentIndex >= numValues) {
            throw new IOException("No more values to read");
        }
        for (int k = 0; k < valueBytes.length; k++) {
            int streamOffset = k * numValues;
            valueBytes[k] = data[streamOffset + currentIndex];
        }
        currentIndex++;
    }
}
