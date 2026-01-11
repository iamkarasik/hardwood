/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import dev.morling.hardwood.metadata.CompressionCodec;

/**
 * Factory for creating decompressor instances based on compression codec.
 */
public class DecompressorFactory {

    private static final UncompressedDecompressor UNCOMPRESSED = new UncompressedDecompressor();
    private static final SnappyDecompressor SNAPPY = new SnappyDecompressor();
    private static final GzipDecompressor GZIP = new GzipDecompressor();
    private static final ZstdDecompressor ZSTD = new ZstdDecompressor();
    private static final Lz4Decompressor LZ4 = new Lz4Decompressor();
    private static final Lz4RawDecompressor LZ4_RAW = new Lz4RawDecompressor();

    // Brotli is lazily initialized to avoid loading native library until needed
    private static volatile BrotliDecompressor BROTLI;

    private static BrotliDecompressor getBrotli() {
        if (BROTLI == null) {
            synchronized (DecompressorFactory.class) {
                if (BROTLI == null) {
                    BROTLI = new BrotliDecompressor();
                }
            }
        }
        return BROTLI;
    }

    /**
     * Get a decompressor for the given compression codec.
     *
     * @param codec the compression codec
     * @return the appropriate decompressor
     * @throws UnsupportedOperationException if the codec is not supported
     */
    public static Decompressor getDecompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> UNCOMPRESSED;
            case SNAPPY -> SNAPPY;
            case GZIP -> GZIP;
            case ZSTD -> ZSTD;
            case LZ4 -> LZ4;
            case LZ4_RAW -> LZ4_RAW;
            case BROTLI -> getBrotli();
            case LZO -> throw new UnsupportedOperationException("LZO compression not yet supported");
        };
    }
}
