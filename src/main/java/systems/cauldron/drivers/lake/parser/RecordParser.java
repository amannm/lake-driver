package systems.cauldron.drivers.lake.parser;

import java.io.Closeable;
import java.util.Optional;

public interface RecordParser extends Closeable {
    Optional<Object[]> parseRecord();
}
