/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reading Parquet files with logical data types.
 */
public class LogicalTypesTest {

    @Test
    void testReadAllLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            // Verify schema has logical types
            assertThat(fileReader.getFileSchema().getColumnCount()).isEqualTo(15);
            assertThat(fileReader.getFileSchema().getColumn("name").logicalType()).isNotNull();
            assertThat(fileReader.getFileSchema().getColumn("birth_date").logicalType()).isNotNull();
            assertThat(fileReader.getFileSchema().getColumn("created_at").logicalType()).isNotNull();

            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                // Verify we read exactly 3 rows
                assertThat(rows).hasSize(3);

                // Row 0: Alice
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                assertThat(row0.getString("name")).isEqualTo("Alice");
                assertThat(row0.getDate("birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));
                assertThat(row0.getTimestamp("created_at")).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));
                assertThat(row0.getTime("wake_time")).isEqualTo(LocalTime.of(7, 30, 0));
                assertThat(row0.getDecimal("balance")).isEqualTo(new BigDecimal("1234.56"));
                assertThat(row0.getInt("tiny_int")).isEqualTo(10);
                assertThat(row0.getInt("small_int")).isEqualTo(1000);
                assertThat(row0.getInt("medium_int")).isEqualTo(100000);
                assertThat(row0.getLong("big_int")).isEqualTo(10000000000L);
                assertThat(row0.getInt("tiny_uint")).isEqualTo(255);
                assertThat(row0.getInt("small_uint")).isEqualTo(65535);
                assertThat(row0.getInt("medium_uint")).isEqualTo(2147483647);
                assertThat(row0.getLong("big_uint")).isEqualTo(9223372036854775807L);
                assertThat(row0.getUuid("account_id")).isEqualTo(UUID.fromString("12345678-1234-5678-1234-567812345678"));

                // Row 1: Bob
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);
                assertThat(row1.getString("name")).isEqualTo("Bob");
                assertThat(row1.getDate("birth_date")).isEqualTo(LocalDate.of(1985, 6, 30));
                assertThat(row1.getTimestamp("created_at")).isEqualTo(Instant.parse("2025-01-02T14:45:30Z"));
                assertThat(row1.getTime("wake_time")).isEqualTo(LocalTime.of(8, 0, 0));
                assertThat(row1.getDecimal("balance")).isEqualTo(new BigDecimal("9876.54"));
                assertThat(row1.getInt("tiny_int")).isEqualTo(20);
                assertThat(row1.getInt("small_int")).isEqualTo(2000);
                assertThat(row1.getInt("medium_int")).isEqualTo(200000);
                assertThat(row1.getLong("big_int")).isEqualTo(20000000000L);
                assertThat(row1.getInt("tiny_uint")).isEqualTo(128);
                assertThat(row1.getInt("small_uint")).isEqualTo(32768);
                assertThat(row1.getInt("medium_uint")).isEqualTo(1000000);
                assertThat(row1.getLong("big_uint")).isEqualTo(5000000000000000000L);
                assertThat(row1.getUuid("account_id")).isEqualTo(UUID.fromString("87654321-4321-8765-4321-876543218765"));

                // Row 2: Charlie
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);
                assertThat(row2.getString("name")).isEqualTo("Charlie");
                assertThat(row2.getDate("birth_date")).isEqualTo(LocalDate.of(2000, 12, 25));
                assertThat(row2.getTimestamp("created_at")).isEqualTo(Instant.parse("2025-01-03T09:15:45Z"));
                assertThat(row2.getTime("wake_time")).isEqualTo(LocalTime.of(6, 45, 0));
                assertThat(row2.getDecimal("balance")).isEqualTo(new BigDecimal("5555.55"));
                assertThat(row2.getInt("tiny_int")).isEqualTo(30);
                assertThat(row2.getInt("small_int")).isEqualTo(3000);
                assertThat(row2.getInt("medium_int")).isEqualTo(300000);
                assertThat(row2.getLong("big_int")).isEqualTo(30000000000L);
                assertThat(row2.getInt("tiny_uint")).isEqualTo(64);
                assertThat(row2.getInt("small_uint")).isEqualTo(16384);
                assertThat(row2.getInt("medium_uint")).isEqualTo(500000);
                assertThat(row2.getLong("big_uint")).isEqualTo(4611686018427387904L);
                assertThat(row2.getUuid("account_id")).isEqualTo(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
            }
        }
    }

    @Test
    void testGetObjectWithLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                Row row = rowReader.iterator().next();

                // Test getObject() returns converted logical types
                assertThat(row.getObject("name")).isInstanceOf(String.class);
                assertThat(row.getObject("birth_date")).isInstanceOf(LocalDate.class);
                assertThat(row.getObject("created_at")).isInstanceOf(Instant.class);
                assertThat(row.getObject("wake_time")).isInstanceOf(LocalTime.class);
                assertThat(row.getObject("balance")).isInstanceOf(BigDecimal.class);
                assertThat(row.getObject("account_id")).isInstanceOf(UUID.class);

                // Verify values match
                assertThat(row.getObject("name")).isEqualTo("Alice");
                assertThat(row.getObject("birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));
            }
        }
    }

    @Test
    void testLogicalTypeMetadata() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();

            // Verify logical types are parsed correctly
            assertThat(schema.getColumn("name").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.StringType.class);
            assertThat(schema.getColumn("birth_date").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.DateType.class);
            assertThat(schema.getColumn("created_at").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.TimestampType.class);
            assertThat(schema.getColumn("wake_time").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.TimeType.class);
            assertThat(schema.getColumn("balance").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.DecimalType.class);
            assertThat(schema.getColumn("tiny_int").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.IntType.class);
            assertThat(schema.getColumn("small_int").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.IntType.class);
            assertThat(schema.getColumn("tiny_uint").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.IntType.class);
            assertThat(schema.getColumn("small_uint").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.IntType.class);
            assertThat(schema.getColumn("medium_uint").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.IntType.class);
            assertThat(schema.getColumn("big_uint").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.IntType.class);
            // medium_int and big_int don't have logical type annotations (PyArrow doesn't write INT_32/INT_64)
            assertThat(schema.getColumn("medium_int").logicalType()).isNull();
            assertThat(schema.getColumn("big_int").logicalType()).isNull();
            // account_id has UUID logical type (PyArrow 21+ writes UUID annotation)
            assertThat(schema.getColumn("account_id").logicalType()).isInstanceOf(
                    dev.morling.hardwood.metadata.LogicalType.UuidType.class);

            // Verify parameterized logical types have correct parameters
            var timestampType = (dev.morling.hardwood.metadata.LogicalType.TimestampType) schema.getColumn("created_at")
                    .logicalType();
            assertThat(timestampType.unit()).isEqualTo(
                    dev.morling.hardwood.metadata.LogicalType.TimestampType.TimeUnit.MILLIS);
            assertThat(timestampType.isAdjustedToUTC()).isTrue();

            var decimalType = (dev.morling.hardwood.metadata.LogicalType.DecimalType) schema.getColumn("balance")
                    .logicalType();
            assertThat(decimalType.scale()).isEqualTo(2);
            assertThat(decimalType.precision()).isEqualTo(10);

            var intType = (dev.morling.hardwood.metadata.LogicalType.IntType) schema.getColumn("tiny_int").logicalType();
            assertThat(intType.bitWidth()).isEqualTo(8);
            assertThat(intType.isSigned()).isTrue();

            // Verify unsigned integer types
            var tinyUintType = (dev.morling.hardwood.metadata.LogicalType.IntType) schema.getColumn("tiny_uint")
                    .logicalType();
            assertThat(tinyUintType.bitWidth()).isEqualTo(8);
            assertThat(tinyUintType.isSigned()).isFalse();

            var smallUintType = (dev.morling.hardwood.metadata.LogicalType.IntType) schema.getColumn("small_uint")
                    .logicalType();
            assertThat(smallUintType.bitWidth()).isEqualTo(16);
            assertThat(smallUintType.isSigned()).isFalse();

            var mediumUintType = (dev.morling.hardwood.metadata.LogicalType.IntType) schema.getColumn("medium_uint")
                    .logicalType();
            assertThat(mediumUintType.bitWidth()).isEqualTo(32);
            assertThat(mediumUintType.isSigned()).isFalse();

            var bigUintType = (dev.morling.hardwood.metadata.LogicalType.IntType) schema.getColumn("big_uint")
                    .logicalType();
            assertThat(bigUintType.bitWidth()).isEqualTo(64);
            assertThat(bigUintType.isSigned()).isFalse();
        }
    }
}
