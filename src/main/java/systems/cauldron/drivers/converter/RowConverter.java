package systems.cauldron.drivers.converter;

import systems.cauldron.drivers.config.TypeSpec;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public abstract class RowConverter {

    public abstract Object[] convertRow(String[] values);

    static Object convertField(TypeSpec fieldType, String string) throws NumberFormatException, DateTimeParseException {
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
}
