package systems.cauldron.drivers.parser;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.converter.RowConverter;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class CsvInputStreamParser implements Closeable {

    private final CsvParser parser;
    private final RowConverter converter;

    public CsvInputStreamParser(FormatSpec spec, RowConverter converter, InputStream inputStream) {

        CsvFormat format = new CsvFormat();
        format.setDelimiter(spec.delimiter);
        format.setLineSeparator(spec.lineSeparator);
        format.setQuote(spec.quoteChar);
        format.setQuoteEscape(spec.escape);

        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(format);
        parserSettings.setHeaderExtractionEnabled(spec.header);

        this.converter = converter;

        this.parser = new CsvParser(parserSettings);
        this.parser.beginParsing(inputStream, StandardCharsets.UTF_8);
    }

    public Optional<Object[]> parseRecord() {
        String[] strings = parser.parseNext();
        if (strings == null) {
            parser.stopParsing();
            return Optional.empty();
        } else {
            Object[] values = converter.convertRow(strings);
            return Optional.of(values);
        }
    }

    @Override
    public void close() throws IOException {
        if (!parser.getContext().isStopped()) {
            parser.stopParsing();
        }
    }
}
