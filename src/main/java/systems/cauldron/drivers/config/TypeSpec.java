package systems.cauldron.drivers.config;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum TypeSpec {

    STRING(String.class, "string"),
    BOOLEAN(Boolean.class, "boolean"),
    BYTE(Byte.class, "byte"),
    CHARACTER(Character.class, "character"),
    SHORT(Short.class, "short"),
    INTEGER(Integer.class, "integer"),
    LONG(Long.class, "long"),
    FLOAT(Float.class, "float"),
    DOUBLE(Double.class, "double"),
    DATE(java.time.LocalDate.class, "date"),
    TIME(java.time.LocalTime.class, "time"),
    DATETIME(java.time.LocalDateTime.class, "datetime"),
    TIMESTAMP(java.time.Instant.class, "timestamp");

    private final Class clazz;
    private final String stringName;

    private static final Map<String, TypeSpec> MAP = Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(v -> v.stringName, v -> v));

    TypeSpec(Class clazz, String stringName) {
        this.clazz = clazz;
        this.stringName = stringName;
    }

    public RelDataType toType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(clazz);
    }

    public static TypeSpec of(String typeString) {
        return MAP.get(typeString);
    }

    public String toJson() {
        return stringName;
    }

}