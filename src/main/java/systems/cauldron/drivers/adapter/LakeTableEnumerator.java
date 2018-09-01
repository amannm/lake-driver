package systems.cauldron.drivers.adapter;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.linq4j.Enumerator;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;
import systems.cauldron.drivers.provider.LakeScan;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LakeTableEnumerator implements Enumerator<Object[]> {

    private final TypeSpec[] fieldTypes;
    private final int[] projects;

    private final AtomicBoolean cancelFlag;
    private final CsvParser parser;

    private Object[] current;

    public LakeTableEnumerator(LakeScan scan, AtomicBoolean cancelFlag) {
        this.fieldTypes = scan.getTypes();
        this.projects = scan.getProjects();
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
        final Object[] result = new Object[projects.length];
        for (int i = 0; i < projects.length; i++) {
            int columnIndex = projects[i];
            String value = values[columnIndex];
            if (value != null) {
                TypeSpec type = fieldTypes[columnIndex];
                result[i] = convert(type, value);
            }
        }
        return result;
    }

    private static Object convert(TypeSpec fieldType, String string) throws NumberFormatException, DateTimeParseException {
        switch (fieldType) {
            case STRING:
                return string;
            case CHARACTER:
                if (string.length() == 1) {
                    return string.charAt(0);
                } else {
                    throw new IllegalArgumentException("invalid char string value: '" + string + "'");
                }
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