package systems.cauldron.drivers.lake.adapter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class CsvReaderTest {


    @BeforeAll
    public static void setup() {

    }

    @AfterAll
    public static void cleanup() {

    }

    @Test
    public void readIPFromNIO() throws IOException {
        Path inputBuild = Paths.get("src", "test", "resources", "train.csv");
        FileChannel channel = FileChannel.open(inputBuild, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocateDirect(65536);
        bb.clear();

        long offset = 0;

        long lineNumber = 1;
        int linesPerPage = 1000;

        long pageOffset = 0;
        long maxPageSize = 0;
        long maxOffset = channel.size();
        System.out.println("File size: " + maxOffset);

        List<Long> pageStartOffsets = new ArrayList<>();
        pageStartOffsets.add(0L);

        long bytesRead = channel.read(bb);
        while (bytesRead != -1) {
            bb.flip();
            while (bb.hasRemaining()) {
                byte value = bb.get();
                offset++;
                if (value == 0x0A) {
                    lineNumber++;
                    if (lineNumber % linesPerPage == 0) {
                        pageStartOffsets.add(offset);
                        long pageSize = offset - pageOffset;
                        maxPageSize = Math.max(maxPageSize, pageSize);
                        pageOffset = offset;
                    }
                }
            }
            bb.clear();
            bytesRead = channel.read(bb);
        }
        maxPageSize = Math.max(maxPageSize, maxOffset - pageStartOffsets.get(pageStartOffsets.size() - 1));

        System.out.println("number of lines: " + lineNumber);
        System.out.println("number of pages: " + pageStartOffsets.size());
        System.out.println("max page size in bytes: " + maxPageSize);

        double exponent = Math.ceil(Math.log(maxPageSize) / Math.log(2));
        int bufferSize = (int) Math.pow(2, exponent);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        System.out.println("buffer size in bytes: " + bufferSize);

        testRandomPageRead(buffer, pageStartOffsets, maxOffset, 0);
        testRandomPageRead(buffer, pageStartOffsets, maxOffset, new Random().nextInt(pageStartOffsets.size()));


    }

    public static void testRandomPageRead(ByteBuffer buffer, List<Long> pageStartOffsets, long maxOffset, int pageIndex) throws IOException {

        System.out.println("fetching page: " + pageIndex);

        long startMillis = System.currentTimeMillis();

        Path inputBuild = Paths.get("src", "test", "resources", "train.csv");
        FileChannel channel = FileChannel.open(inputBuild, StandardOpenOption.READ);

        long startOffset = pageStartOffsets.get(pageIndex);
        channel.position(startOffset);

        long endOffset = pageIndex + 1 != pageStartOffsets.size() ? pageStartOffsets.get(pageIndex + 1) : maxOffset;
        long pageSize = endOffset - startOffset;
        System.out.println("page size in bytes: " + pageSize);
        buffer.limit((int) pageSize);

        channel.read(buffer);
        buffer.flip();
        if (buffer.hasRemaining()) {
            System.out.println(StandardCharsets.UTF_8.decode(buffer).toString());
        }
        buffer.clear();

        long stopMillis = System.currentTimeMillis();
        long duration = stopMillis - startMillis;
        System.out.println("read 1000 lines in " + duration + "ms");

    }
}