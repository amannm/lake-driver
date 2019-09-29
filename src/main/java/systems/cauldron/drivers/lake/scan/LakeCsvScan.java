package systems.cauldron.drivers.lake.scan;

import systems.cauldron.drivers.lake.config.FormatSpec;
import systems.cauldron.drivers.lake.config.TypeSpec;
import systems.cauldron.drivers.lake.converter.StringRowConverter;
import systems.cauldron.drivers.lake.parser.CsvRecordParser;
import systems.cauldron.drivers.lake.parser.RecordParser;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeCsvScan extends LakeScan {

    final URI source;
    final FormatSpec format;

    LakeCsvScan(TypeSpec[] types, int[] projects, URI source, FormatSpec format) {
        super(types, projects);
        this.source = source;
        this.format = format;
    }

    @Override
    public RecordParser getRecordParser() {
        return new CsvRecordParser(
                this.format,
                this.getStringRowConverter(),
                this.getSourceInputStream());
    }

    public abstract StringRowConverter getStringRowConverter();

    public abstract InputStream getSourceInputStream();

}
