/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.parquet.metadata;

import java.io.IOException;

import dev.morling.hardwood.parquet.Encoding;
import dev.morling.hardwood.parquet.thrift.ThriftCompactReader;

/**
 * Header for DataPage v2.
 */
public record DataPageHeaderV2(
        int numValues,
        int numNulls,
        int numRows,
        Encoding encoding,
        int definitionLevelsByteLength,
        int repetitionLevelsByteLength) {

    public static DataPageHeaderV2 read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static DataPageHeaderV2 readInternal(ThriftCompactReader reader) throws IOException {
        int numValues = 0;
        int numNulls = 0;
        int numRows = 0;
        Encoding encoding = null;
        int definitionLevelsByteLength = 0;
        int repetitionLevelsByteLength = 0;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // num_values
                    numValues = reader.readI32();
                    break;
                case 2: // num_nulls
                    numNulls = reader.readI32();
                    break;
                case 3: // num_rows
                    numRows = reader.readI32();
                    break;
                case 4: // encoding
                    encoding = Encoding.fromThriftValue(reader.readI32());
                    break;
                case 5: // definition_levels_byte_length
                    definitionLevelsByteLength = reader.readI32();
                    break;
                case 6: // repetition_levels_byte_length
                    repetitionLevelsByteLength = reader.readI32();
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        return new DataPageHeaderV2(numValues, numNulls, numRows, encoding,
                definitionLevelsByteLength, repetitionLevelsByteLength);
    }
}
