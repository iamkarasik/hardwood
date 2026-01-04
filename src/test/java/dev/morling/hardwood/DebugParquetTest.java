/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Paths;

import dev.morling.hardwood.parquet.ParquetFileReader;

public class DebugParquetTest {

    public static void main(String[] args) throws Exception {
        String file = "src/test/resources/plain_uncompressed_with_nulls.parquet";

        System.out.println("=== " + file + " ===");
        try (ParquetFileReader reader = ParquetFileReader.open(Paths.get(file))) {
            System.out.println("Version: " + reader.getFileMetaData().version());
            System.out.println("Num rows: " + reader.getFileMetaData().numRows());
            System.out.println("Row groups: " + reader.getFileMetaData().rowGroups().size());
            System.out.println("Schema:");
            System.out.println(reader.getSchema());
            System.out.println();

            // Check encodings for each column
            var rowGroup = reader.getFileMetaData().rowGroups().get(0);
            for (int i = 0; i < rowGroup.columns().size(); i++) {
                var col = rowGroup.columns().get(i);
                System.out.println("Column " + i + ": " + reader.getSchema().getColumn(i).name());
                System.out.println("  Type: " + col.metaData().type());
                System.out.println("  Codec: " + col.metaData().codec());
                System.out.println("  Encodings: " + col.metaData().encodings());
                System.out.println("  Num values: " + col.metaData().numValues());
                System.out.println("  Data page offset: " + col.metaData().dataPageOffset());
                System.out.println("  Dictionary page offset: " + col.metaData().dictionaryPageOffset());
                System.out.println();
            }
        }
    }
}
