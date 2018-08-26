package systems.cauldron.drivers.adapter;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.linq4j.Enumerator;
import systems.cauldron.drivers.config.FormatSpecification;
import systems.cauldron.drivers.provider.LakeProvider;

import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LakeTableEnumerator implements Enumerator<Object[]> {

    private final CsvParser parser;
    private final AtomicBoolean cancelFlag;
    private final LakeFieldType[] fieldTypes;
    private Object[] current;

    public LakeTableEnumerator(FormatSpecification readerConfig, LakeFieldType[] fields, AtomicBoolean cancelFlag, LakeProvider gateway) {
        CsvFormat format = new CsvFormat();
        format.setDelimiter(readerConfig.delimiter);
        format.setLineSeparator(readerConfig.lineSeparator);
        format.setQuote(readerConfig.quoteChar);
        format.setQuoteEscape(readerConfig.escape);
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(format);
        parserSettings.setHeaderExtractionEnabled(readerConfig.header);
        this.parser = new CsvParser(parserSettings);
        InputStream data = gateway.fetchSource();
        this.parser.beginParsing(data);
        this.fieldTypes = fields;
        this.cancelFlag = cancelFlag;
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
        current = convertRow(strings);
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

    private Object[] convertRow(String[] values) {
        final Object[] record = new Object[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            String value = values[i];
            if (value != null) {
                LakeFieldType type = fieldTypes[i];
                record[i] = convert(type, value);
            }
        }
        return record;
    }

    private static Object convert(LakeFieldType fieldType, String string) throws NumberFormatException, DateTimeParseException {
        switch (fieldType) {
            case STRING:
            case CHARACTER:
                return string;
            case BOOLEAN:
                return Boolean.parseBoolean(string);
            case BYTE:
                return Byte.parseByte(string);
            case SHORT:
                return Short.parseShort(string);
            case INTEGER:
                return Integer.parseInt(string);
            case LONG:
                return Long.parseLong(string);
            case FLOAT:
                return Float.parseFloat(string);
            case DOUBLE:
                return Double.parseDouble(string);
            case DATE:
                return DateTimeFormatter.ISO_LOCAL_DATE.parse(string, LocalDate::from);
            case TIME:
                return DateTimeFormatter.ISO_LOCAL_TIME.parse(string, LocalTime::from);
            case DATETIME:
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(string, LocalDateTime::from);
            case TIMESTAMP:
                return DateTimeFormatter.ISO_INSTANT.parse(string, Instant::from);
            default:
                throw new IllegalArgumentException("invalid field type: " + fieldType.toString());
        }
    }


}