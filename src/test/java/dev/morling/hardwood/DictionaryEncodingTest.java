/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.reader.ColumnReader;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reading dictionary encoded Parquet files.
 */
class DictionaryEncodingTest {

    @Test
    void testReadDictionaryEncodedFile() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            assertThat(reader).isNotNull();

            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.numRows()).isEqualTo(5);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            assertThat(reader.getFileSchema().getColumnCount()).isEqualTo(2);

            // Read row group
            RowGroup rowGroup = metadata.rowGroups().get(0);
            assertThat(rowGroup.columns()).hasSize(2);

            // Read and verify 'id' column (PLAIN encoded)
            ColumnChunk idColumnChunk = rowGroup.columns().get(0);
            ColumnSchema idColumn = reader.getFileSchema().getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");

            ColumnReader idReader = reader.getColumnReader(idColumn, idColumnChunk);
            List<Object> idValues = idReader.readAll();
            assertThat(idValues).hasSize(5);
            assertThat(idValues).containsExactly(1L, 2L, 3L, 4L, 5L);

            // Read and verify 'category' column (DICTIONARY encoded)
            ColumnChunk categoryColumnChunk = rowGroup.columns().get(1);
            ColumnSchema categoryColumn = reader.getFileSchema().getColumn(1);
            assertThat(categoryColumn.name()).isEqualTo("category");

            // Verify dictionary encoding is used
            assertThat(categoryColumnChunk.metaData().encodings())
                    .contains(dev.morling.hardwood.metadata.Encoding.RLE_DICTIONARY);

            ColumnReader categoryReader = reader.getColumnReader(categoryColumn, categoryColumnChunk);
            List<Object> categoryValues = categoryReader.readAll();
            assertThat(categoryValues).hasSize(5);

            // Verify the exact values: ['A', 'B', 'A', 'C', 'B']
            assertThat(new String((byte[]) categoryValues.get(0))).isEqualTo("A");
            assertThat(new String((byte[]) categoryValues.get(1))).isEqualTo("B");
            assertThat(new String((byte[]) categoryValues.get(2))).isEqualTo("A");
            assertThat(new String((byte[]) categoryValues.get(3))).isEqualTo("C");
            assertThat(new String((byte[]) categoryValues.get(4))).isEqualTo("B");
        }
    }
}
