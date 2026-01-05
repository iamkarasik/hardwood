# Hardwood

A minimal dependency parser for the Apache Parquet file format.

## Project Vision

Now:

* Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* Be correct: Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library

In the future:

* Be fast: as fast or faster as parquet-java
* Add a Parquet file writer

## Build

This project requires Java 17 or newer for building.
It comes with the Apache [Maven wrapper](https://github.com/takari/maven-wrapper),
i.e. a Maven distribution will be downloaded automatically, if needed.

Run the following command to build this project:

```shell
./mvnw clean verify
```

On Windows, run the following command:

```shell
mvnw.cmd clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```shell
./mvnw clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
./mvnw process-sources
```

## License

This code base is available under the Apache License, version 2.
