/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Paths;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.Row;

public class DebugParquetTest {

    public static void main(String[] args) throws Exception {
        String file = "src/test/resources/yellow_tripdata_2025-01.parquet";

        System.out.println("=== " + file + " ===");
        try (ParquetFileReader reader = ParquetFileReader.open(Paths.get(file))) {
            System.out.println("Version: " + reader.getFileMetaData().version());
            System.out.println("Num rows: " + reader.getFileMetaData().numRows());
            System.out.println("Row groups: " + reader.getFileMetaData().rowGroups().size());
            System.out.println("Created by: " + reader.getFileMetaData().createdBy());
            System.out.println();

            // Show file schema with logical types
            System.out.println("File Schema:");
            System.out.println(reader.getFileSchema());
            System.out.println();

            // Show first 3 columns in detail
            System.out.println("Column Details:");
            for (int i = 0; i < Math.min(3, reader.getFileSchema().getColumnCount()); i++) {
                var col = reader.getFileSchema().getColumn(i);
                System.out.println("  Column " + i + ": " + col.name());
                System.out.println("    Physical: " + col.type());
                System.out.println("    Logical: " + col.logicalType());
                System.out.println();
            }

            try (RowReader rowReader = reader.createRowReader()) {
                System.out.println("Reading first 5 rows with timestamp conversion:");
                int i = 0;
                for (Row row : rowReader) {
                    System.out.println(String.format("Row %d: VendorID=%s, Pickup=%s, Dropoff=%s",
                            i,
                            row.isNull(0) ? "null" : row.getInt(0),
                            row.isNull(1) ? "null" : row.getTimestamp(1),
                            row.isNull(2) ? "null" : row.getTimestamp(2)));
                    if (i >= 4) {
                        break;
                    }
                    i++;
                }
            }
        }
    }
}
