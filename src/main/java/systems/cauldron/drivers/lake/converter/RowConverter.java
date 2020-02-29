package systems.cauldron.drivers.lake.converter;

import systems.cauldron.drivers.lake.config.TypeSpec;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public abstract class RowConverter {

    public abstract Object[] convertRow(String[] values);

    static Object convertField(TypeSpec type, String value) throws NumberFormatException, DateTimeParseException {
        switch (type) {
            case STRING:
                return value;
            case CHARACTER:
                if (value.length() == 1) {
                    return value.charAt(0);
                } else {
                    throw new IllegalArgumentException("invalid char string value: '" + value + "'");
                }
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case BYTE:
                return Byte.parseByte(value);
            case SHORT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case DATE:
                return DateTimeFormatter.ISO_LOCAL_DATE.parse(value, LocalDate::from);
            case TIME:
                return DateTimeFormatter.ISO_LOCAL_TIME.parse(value, LocalTime::from);
            case DATETIME:
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(value, LocalDateTime::from);
            case TIMESTAMP:
                return DateTimeFormatter.ISO_INSTANT.parse(value, Instant::from);
            default:
                throw new IllegalArgumentException("invalid field type: " + type);
        }
    }
}
