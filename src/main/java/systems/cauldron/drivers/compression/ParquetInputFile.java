package systems.cauldron.drivers.compression;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class ParquetInputFile implements InputFile {

    private final Path source;

    public ParquetInputFile(Path source) {
        this.source = source;
    }

    @Override
    public long getLength() throws IOException {
        return Files.size(source);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        FileChannel channel = FileChannel.open(source,
                StandardOpenOption.READ);
        return new ByteChannelSeekableInputStream(channel);
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
