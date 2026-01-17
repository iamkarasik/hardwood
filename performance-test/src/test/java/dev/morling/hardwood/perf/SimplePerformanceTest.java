/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.perf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;

/**
 * Performance comparison test between Hardwood, and parquet-java.
 *
 * <p>Downloads NYC Yellow Taxi Trip Records for 2025 and compares reading
 * performance while verifying correctness by comparing calculated sums.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimplePerformanceTest {

    private static final String BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/";
    private static final Path DATA_DIR = Path.of("target/tlc-trip-record-data");
    private static final int YEAR = 2025;

    record Result(long passengerCount, double tripDistance, double fareAmount, long durationMs, long rowCount) {
    }

    @BeforeAll
    void downloadData() throws IOException {
        Files.createDirectories(DATA_DIR);
        for (int month = 1; month <= 12; month++) {
            String filename = String.format("yellow_tripdata_%d-%02d.parquet", YEAR, month);
            Path target = DATA_DIR.resolve(filename);
            if (!Files.exists(target)) {
                downloadFile(BASE_URL + filename, target);
            }
        }
    }

    @Test
    void comparePerformance() throws IOException {
        List<Path> files = getAvailableFiles();
        assertThat(files).as("At least one data file should be available").isNotEmpty();

        System.out.println("\n=== Performance Test ===");
        System.out.println("Files available: " + files.size());

        // Warmup run (not timed)
        System.out.println("\nWarmup run...");
        runHardwood(files);

        // Timed runs
        System.out.println("\nTimed runs:");
        Result hardwoodResult = timeRun("Hardwood", () -> runHardwood(files));
        Result parquetJavaResult = timeRun("parquet-java", () -> runParquetJava(files));

        // Print results
        printResults(files.size(), hardwoodResult, parquetJavaResult);

        // Verify correctness - compare against parquet-java as reference
        assertThat(hardwoodResult.passengerCount())
                .as("Hardwood passenger_count should match parquet-java")
                .isEqualTo(parquetJavaResult.passengerCount());
        assertThat(hardwoodResult.tripDistance())
                .as("Hardwood trip_distance should match parquet-java")
                .isCloseTo(parquetJavaResult.tripDistance(), within(0.001));
        assertThat(hardwoodResult.fareAmount())
                .as("Hardwood fare_amount should match parquet-java")
                .isCloseTo(parquetJavaResult.fareAmount(), within(0.001));

        System.out.println("\nAll results match!");
    }

    private List<Path> getAvailableFiles() throws IOException {
        List<Path> files = new ArrayList<>();
        for (int month = 1; month <= 3; month++) {
            String filename = String.format("yellow_tripdata_%d-%02d.parquet", YEAR, month);
            Path file = DATA_DIR.resolve(filename);
            if (Files.exists(file) && Files.size(file) > 0) {
                files.add(file);
            }
        }
        return files;
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        System.out.println("  Running " + name + "...");
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        return new Result(result.passengerCount(), result.tripDistance(),
                result.fareAmount(), duration, result.rowCount());
    }

    private Result runHardwood(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        for (Path file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    RowReader rowReader = reader.createRowReader()) {
                for (PqRow row : rowReader) {
                    rowCount++;
                    Long pc = row.getValue(PqType.INT64, "passenger_count");
                    if (pc != null) {
                        passengerCount += pc;
                    }

                    Double td = row.getValue(PqType.DOUBLE, "trip_distance");
                    if (td != null) {
                        tripDistance += td;
                    }

                    Double fa = row.getValue(PqType.DOUBLE, "fare_amount");
                    if (fa != null) {
                        fareAmount += fa;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read file: " + file, e);
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private Result runParquetJava(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;
        Configuration conf = new Configuration();

        for (Path file : files) {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord> builder(HadoopInputFile.fromPath(hadoopPath, conf))
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    rowCount++;
                    Long pc = (Long) record.get("passenger_count");
                    if (pc != null) {
                        passengerCount += pc;
                    }

                    Double td = (Double) record.get("trip_distance");
                    if (td != null) {
                        tripDistance += td;
                    }

                    Double fa = (Double) record.get("fare_amount");
                    if (fa != null) {
                        fareAmount += fa;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read file: " + file, e);
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private void printResults(int fileCount, Result hardwood, Result parquetJava) {
        System.out.println("\n=== Performance Test Results ===");
        System.out.println("Files processed: " + fileCount);
        System.out.println("Total rows: " + String.format("%,d", hardwood.rowCount()));
        System.out.println();
        System.out.println(String.format("%-20s | %10s | %17s | %17s | %17s",
                "Contender", "Time (s)", "passenger_count", "trip_distance", "fare_amount"));
        System.out.println("-".repeat(20) + "-+-" + "-".repeat(10) + "-+-" + "-".repeat(17)
                + "-+-" + "-".repeat(17) + "-+-" + "-".repeat(17));

        printResultRow("Hardwood", hardwood);
        printResultRow("parquet-java", parquetJava);
    }

    private void printResultRow(String name, Result result) {
        System.out.println(String.format("%-20s | %10.2f | %,17d | %,17.2f | %,17.2f",
                name,
                result.durationMs() / 1000.0,
                result.passengerCount(),
                result.tripDistance(),
                result.fareAmount()));
    }

    private void downloadFile(String url, Path target) {
        System.out.println("Downloading: " + url);
        try {
            HttpClient client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<Path> response = client.send(request,
                    HttpResponse.BodyHandlers.ofFile(target));
            if (response.statusCode() != 200) {
                Files.deleteIfExists(target);
                System.out.println("  Failed (status " + response.statusCode() + ") - skipping");
            }
            else {
                System.out.println("  Downloaded: " + Files.size(target) + " bytes");
            }
        }
        catch (Exception e) {
            System.out.println("  Failed: " + e.getMessage() + " - skipping");
            try {
                Files.deleteIfExists(target);
            }
            catch (IOException ignored) {
            }
        }
    }
}
