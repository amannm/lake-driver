package systems.cauldron.drivers.adapter;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.linq4j.Enumerator;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.converter.RowConverter;
import systems.cauldron.drivers.scan.LakeScan;

import java.util.concurrent.atomic.AtomicBoolean;

public class LakeTableEnumerator implements Enumerator<Object[]> {

    private final AtomicBoolean cancelFlag;
    private final RowConverter converter;
    private final CsvParser parser;

    private Object[] current;

    LakeTableEnumerator(LakeScan scan, AtomicBoolean cancelFlag) {
        this.converter = scan.getRowConverter();
        this.cancelFlag = cancelFlag;
        this.parser = new CsvParser(getParserSettings(scan.getFormat()));
        this.parser.beginParsing(scan.getSource());
    }

    public Object[] current() {
        return current;
    }

    public boolean moveNext() {
        if (cancelFlag.get()) {
            return false;
        }
        final String[] strings = parser.parseNext();
        if (strings == null) {
            current = null;
            parser.stopParsing();
            return false;
        }
        current = converter.convertRow(strings);
        return true;
    }

    public void reset() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        if (!parser.getContext().isStopped()) {
            parser.stopParsing();
        }
    }

    private static CsvParserSettings getParserSettings(FormatSpec spec) {
        CsvFormat format = new CsvFormat();
        format.setDelimiter(spec.delimiter);
        format.setLineSeparator(spec.lineSeparator);
        format.setQuote(spec.quoteChar);
        format.setQuoteEscape(spec.escape);
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(format);
        parserSettings.setHeaderExtractionEnabled(spec.header);
        return parserSettings;
    }

}