package dev.hardwood.reader.s3;

import java.io.IOException;
import java.net.URI;

import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ParquetFileReader implements ParquetFileReader {

    private final URI location;
    private final S3Client client;
    private final HardwoodContextImpl context;
    private final boolean ownsContext;

    private S3ParquetFileReader(URI location, S3Client client,HardwoodContextImpl context, boolean ownsContext) {
        this.location = location;
        this.client = client;
        this.context = context;
        this.ownsContext = ownsContext;
    }

    public static ParquetFileReader open(URI location, S3Client client, HardwoodContext context) {
        return new S3ParquetFileReader(location, client, (HardwoodContextImpl) context, false);
    }

    public static ParquetFileReader open(URI location, S3Client client) {
        HardwoodContextImpl context = HardwoodContextImpl.create();
        return new S3ParquetFileReader(location, client, context, true);
    }

    @Override
    public FileMetaData getFileMetaData() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFileMetaData'");
    }

    @Override
    public FileSchema getFileSchema() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFileSchema'");
    }

    @Override
    public ColumnReader createColumnReader(String columnName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createColumnReader'");
    }

    @Override
    public ColumnReader createColumnReader(int columnIndex) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createColumnReader'");
    }

    @Override
    public RowReader createRowReader() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createRowReader'");
    }

    @Override
    public RowReader createRowReader(ColumnProjection projection) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createRowReader'");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }
}
