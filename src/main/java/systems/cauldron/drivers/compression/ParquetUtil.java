package systems.cauldron.drivers.compression;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;


public class ParquetUtil {

    public static void readFromParquet(Path filePathToRead) throws IOException {


        InputFile inputFile = new InputFile() {

            @Override
            public long getLength() throws IOException {
                return Files.size(filePathToRead);
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                FileChannel channel = FileChannel.open(filePathToRead,
                        StandardOpenOption.READ);
                return new ByteChannelSeekableInputStream(channel);
            }
        };

        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(inputFile)
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }

    public static void writeToParquet(String avscJsonString, List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {

        Schema schema = new Schema.Parser().parse(avscJsonString);

        OutputFile outputFile = new OutputFile() {

            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                FileChannel channel = FileChannel.open(fileToWrite,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW);
                return new ByteChannelPositionOutputStream(channel);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                FileChannel channel = FileChannel.open(fileToWrite,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                return new ByteChannelPositionOutputStream(channel);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

        };

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }


    private static class ByteChannelPositionOutputStream extends PositionOutputStream {

        private final SeekableByteChannel channel;

        private ByteChannelPositionOutputStream(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void write(int b) throws IOException {
            byte[] bytes = new byte[1];
            ByteBuffer wrap = ByteBuffer.wrap(bytes);
            wrap.put((byte) b);
            wrap.flip();
            channel.write(wrap);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

    }

    private static class ByteChannelSeekableInputStream extends SeekableInputStream {

        private final SeekableByteChannel channel;

        private ByteChannelSeekableInputStream(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public int read() throws IOException {
            byte[] bytes = new byte[1];
            ByteBuffer wrap = ByteBuffer.wrap(bytes);
            channel.read(wrap);
            wrap.flip();
            return wrap.get();
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            channel.position(newPos);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            checkBytesAvailable(channel, bytes.length);
            ByteBuffer wrap = ByteBuffer.wrap(bytes);
            channel.read(wrap);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            checkBytesAvailable(channel, len);
            ByteBuffer wrap = ByteBuffer.wrap(bytes);
            wrap.position(start);
            wrap.limit(start + len);
            channel.read(wrap);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            return channel.read(buf);
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            checkBytesAvailable(channel, buf.remaining());
            channel.read(buf);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        private static void checkBytesAvailable(SeekableByteChannel channel, long length) throws IOException {
            long fileBytesRemaining = channel.size() - channel.position();
            if (fileBytesRemaining < length) {
                throw new EOFException();
            }
        }

    }

}