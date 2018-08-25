package systems.cauldron.function.build.adapter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

//TODO: don't invent new meta-config-language-stuff
public enum LakeFieldType {

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
    private final String simpleName;

    private static final Map<String, LakeFieldType> MAP = Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(v -> v.simpleName, v -> v));

    LakeFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(clazz);
    }

    public static LakeFieldType of(String typeString) {
        return MAP.get(typeString);
    }
}