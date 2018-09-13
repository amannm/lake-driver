package systems.cauldron.drivers.compression;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class ParquetOutputFile implements OutputFile {

    private final Path destination;

    ParquetOutputFile(Path destination) {
        this.destination = destination;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        FileChannel channel = FileChannel.open(destination,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
        return new ByteChannelPositionOutputStream(channel);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        FileChannel channel = FileChannel.open(destination,
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
}
