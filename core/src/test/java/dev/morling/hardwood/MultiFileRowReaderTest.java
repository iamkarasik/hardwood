/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ColumnProjection;
import dev.morling.hardwood.reader.Hardwood;
import dev.morling.hardwood.reader.MultiFileRowReader;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for MultiFileRowReader and cross-file prefetching.
 */
class MultiFileRowReaderTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");
    private static final Path TEST_FILE_WITH_NULLS = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

    @Test
    void testReadSingleFile() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            List<Long> ids = new ArrayList<>();
            List<Long> values = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getLong("id"));
                values.add(reader.getLong("value"));
            }

            assertThat(ids).containsExactly(1L, 2L, 3L);
            assertThat(values).containsExactly(100L, 200L, 300L);
        }
    }

    @Test
    void testReadMultipleIdenticalFiles() throws Exception {
        // Read the same file multiple times to test cross-file prefetching
        List<Path> files = List.of(TEST_FILE, TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            List<Long> ids = new ArrayList<>();
            List<Long> values = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getLong("id"));
                values.add(reader.getLong("value"));
            }

            // Should have 9 rows total (3 rows x 3 files)
            assertThat(ids).hasSize(9);
            assertThat(values).hasSize(9);

            // Verify values repeat as expected
            assertThat(ids).containsExactly(1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 3L);
            assertThat(values).containsExactly(100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L);
        }
    }

    @Test
    void testReadMultipleFilesWithProjection() throws Exception {
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files, ColumnProjection.columns("id"))) {

            List<Long> ids = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getLong("id"));
            }

            // Should have 6 rows total (3 rows x 2 files)
            assertThat(ids).hasSize(6);
            assertThat(ids).containsExactly(1L, 2L, 3L, 1L, 2L, 3L);
        }
    }

    @Test
    void testFieldCount() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            assertThat(reader.getFieldCount()).isEqualTo(2);
            assertThat(reader.getFieldName(0)).isEqualTo("id");
            assertThat(reader.getFieldName(1)).isEqualTo("value");
        }
    }

    @Test
    void testFieldCountWithProjection() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files, ColumnProjection.columns("value"))) {

            assertThat(reader.getFieldCount()).isEqualTo(1);
            assertThat(reader.getFieldName(0)).isEqualTo("value");
        }
    }

    @Test
    void testAccessByIndex() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            reader.hasNext();
            reader.next();

            // Access by projected index
            assertThat(reader.getLong(0)).isEqualTo(1L); // id
            assertThat(reader.getLong(1)).isEqualTo(100L); // value
        }
    }

    @Test
    void testEmptyFileListThrows() {
        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> hardwood.openAll(List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("At least one file");
        }
    }

    @Test
    void testRowCountMatchesSingleFileReading() throws Exception {
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        // Count rows using MultiFileRowReader
        long multiFileCount = 0;
        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {
            while (reader.hasNext()) {
                reader.next();
                multiFileCount++;
            }
        }

        // Count rows using individual readers
        long singleFileCount = 0;
        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader fileReader = hardwood.open(file);
                     RowReader rowReader = fileReader.createRowReader()) {
                    while (rowReader.hasNext()) {
                        rowReader.next();
                        singleFileCount++;
                    }
                }
            }
        }

        assertThat(multiFileCount).isEqualTo(singleFileCount);
    }

    @Test
    void testReadFileWithNulls() throws Exception {
        List<Path> files = List.of(TEST_FILE_WITH_NULLS);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            int rowCount = 0;
            int nullCount = 0;

            while (reader.hasNext()) {
                reader.next();
                rowCount++;
                // The file has "id" and "name" columns, with "name" having nulls
                if (reader.isNull("name")) {
                    nullCount++;
                }
            }

            assertThat(rowCount).isEqualTo(3);
            // Second row has null name
            assertThat(nullCount).isEqualTo(1);
        }
    }

    @Test
    void testReadMultipleFilesPreservesDataIntegrity() throws Exception {
        // Test that data is not corrupted across file boundaries
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            long runningSum = 0;
            int rowCount = 0;

            while (reader.hasNext()) {
                reader.next();
                long id = reader.getLong("id");
                long value = reader.getLong("value");

                // Verify the relationship holds (value = id * 100 in this test file)
                assertThat(value).isEqualTo(id * 100);

                runningSum += value;
                rowCount++;
            }

            assertThat(rowCount).isEqualTo(6);
            // Sum should be (100 + 200 + 300) * 2 = 1200
            assertThat(runningSum).isEqualTo(1200L);
        }
    }

    // ==================== Non-Flat Schema Tests ====================

    private static final Path NESTED_STRUCT_FILE = Paths.get("src/test/resources/nested_struct_test.parquet");

    @Test
    void testReadNestedStructSingleFile() throws Exception {
        List<Path> files = List.of(NESTED_STRUCT_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            // Row 0: id=1, address={street="123 Main St", city="New York", zip=10001}
            assertThat(reader.hasNext()).isTrue();
            reader.next();
            assertThat(reader.getInt("id")).isEqualTo(1);
            PqStruct address0 = reader.getStruct("address");
            assertThat(address0).isNotNull();
            assertThat(address0.getString("street")).isEqualTo("123 Main St");
            assertThat(address0.getString("city")).isEqualTo("New York");
            assertThat(address0.getInt("zip")).isEqualTo(10001);

            // Row 1: id=2, address={street="456 Oak Ave", city="Los Angeles", zip=90001}
            assertThat(reader.hasNext()).isTrue();
            reader.next();
            assertThat(reader.getInt("id")).isEqualTo(2);
            PqStruct address1 = reader.getStruct("address");
            assertThat(address1).isNotNull();
            assertThat(address1.getString("street")).isEqualTo("456 Oak Ave");
            assertThat(address1.getString("city")).isEqualTo("Los Angeles");
            assertThat(address1.getInt("zip")).isEqualTo(90001);

            // Row 2: id=3, address=null
            assertThat(reader.hasNext()).isTrue();
            reader.next();
            assertThat(reader.getInt("id")).isEqualTo(3);
            assertThat(reader.isNull("address")).isTrue();
            assertThat(reader.getStruct("address")).isNull();

            assertThat(reader.hasNext()).isFalse();
        }
    }

    @Test
    void testReadNestedStructMultipleFiles() throws Exception {
        // Read the same nested struct file multiple times to test cross-file transitions
        List<Path> files = List.of(NESTED_STRUCT_FILE, NESTED_STRUCT_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            List<Integer> ids = new ArrayList<>();
            List<String> cities = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getInt("id"));

                PqStruct address = reader.getStruct("address");
                if (address != null) {
                    cities.add(address.getString("city"));
                }
                else {
                    cities.add(null);
                }
            }

            // Should have 6 rows total (3 rows x 2 files)
            assertThat(ids).hasSize(6);
            assertThat(ids).containsExactly(1, 2, 3, 1, 2, 3);

            // Cities: New York, Los Angeles, null, New York, Los Angeles, null
            assertThat(cities).containsExactly(
                    "New York", "Los Angeles", null,
                    "New York", "Los Angeles", null
            );
        }
    }

    @Test
    void testReadNestedStructWithProjection() throws Exception {
        // Project only the nested struct column (not the id)
        List<Path> files = List.of(NESTED_STRUCT_FILE, NESTED_STRUCT_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files, ColumnProjection.columns("address"))) {

            assertThat(reader.getFieldCount()).isEqualTo(1);
            assertThat(reader.getFieldName(0)).isEqualTo("address");

            List<String> streets = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                PqStruct address = reader.getStruct("address");
                if (address != null) {
                    streets.add(address.getString("street"));
                }
                else {
                    streets.add(null);
                }
            }

            // Should have 6 rows total
            assertThat(streets).hasSize(6);
            assertThat(streets).containsExactly(
                    "123 Main St", "456 Oak Ave", null,
                    "123 Main St", "456 Oak Ave", null
            );
        }
    }

    @Test
    void testNestedStructRowCountMatchesSingleFileReading() throws Exception {
        List<Path> files = List.of(NESTED_STRUCT_FILE, NESTED_STRUCT_FILE);

        // Count rows using MultiFileRowReader
        long multiFileCount = 0;
        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {
            while (reader.hasNext()) {
                reader.next();
                multiFileCount++;
            }
        }

        // Count rows using individual readers
        long singleFileCount = 0;
        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader fileReader = hardwood.open(file);
                     RowReader rowReader = fileReader.createRowReader()) {
                    while (rowReader.hasNext()) {
                        rowReader.next();
                        singleFileCount++;
                    }
                }
            }
        }

        assertThat(multiFileCount).isEqualTo(singleFileCount);
    }
}
