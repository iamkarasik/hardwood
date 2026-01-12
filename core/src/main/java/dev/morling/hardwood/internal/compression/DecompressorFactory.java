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

    /**
     * Get a decompressor for the given compression codec.
     *
     * @param codec the compression codec
     * @return the appropriate decompressor
     * @throws UnsupportedOperationException if the codec is not supported or the required library is missing
     */
    public static Decompressor getDecompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new UncompressedDecompressor();
            case GZIP -> new GzipDecompressor();
            case SNAPPY -> {
                checkClassAvailable("org.xerial.snappy.Snappy",
                        "SNAPPY",
                        "org.xerial.snappy:snappy-java");
                yield new SnappyDecompressor();
            }
            case ZSTD -> {
                checkClassAvailable("com.github.luben.zstd.Zstd",
                        "ZSTD",
                        "com.github.luben:zstd-jni");
                yield new ZstdDecompressor();
            }
            case LZ4 -> {
                checkClassAvailable("net.jpountz.lz4.LZ4Factory",
                        "LZ4",
                        "org.lz4:lz4-java");
                yield new Lz4Decompressor();
            }
            case LZ4_RAW -> {
                checkClassAvailable("net.jpountz.lz4.LZ4Factory",
                        "LZ4_RAW",
                        "org.lz4:lz4-java");
                yield new Lz4RawDecompressor();
            }
            case BROTLI -> {
                checkClassAvailable("com.aayushatharva.brotli4j.Brotli4jLoader",
                        "BROTLI",
                        "com.aayushatharva.brotli4j:brotli4j");
                yield new BrotliDecompressor();
            }
            case LZO -> throw new UnsupportedOperationException("LZO compression is not supported");
        };
    }

    private static void checkClassAvailable(String className, String codecName, String dependency) {
        try {
            Class.forName(className);
        }
        catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(
                    "Cannot read " + codecName + "-compressed Parquet file: required library not found. " +
                            "Add the following dependency to your project: " + dependency);
        }
    }
}
