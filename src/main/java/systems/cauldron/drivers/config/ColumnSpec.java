package systems.cauldron.drivers.config;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import javax.json.Json;
import javax.json.JsonObject;

public class ColumnSpec {

    public final String label;
    public final TypeSpec datatype;
    public final Boolean nullable;

    public ColumnSpec(String label, TypeSpec datatype, Boolean nullable) {
        this.label = label;
        this.datatype = datatype;
        this.nullable = nullable;
    }

    public ColumnSpec(JsonObject object) {
        this.label = object.getString("label");
        this.datatype = TypeSpec.of(object.getString("datatype"));
        this.nullable = object.getBoolean("nullable");
    }

    public JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("label", label)
                .add("datatype", datatype.toJson())
                .add("nullable", nullable)
                .build();
    }

    public SchemaBuilder.FieldAssembler<Schema> handleBuilder(SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {

        SchemaBuilder.FieldTypeBuilder<Schema> type = fieldAssembler.name(label).type();

        SchemaBuilder.BaseFieldTypeBuilder<Schema> baseType;
        if (nullable) {
            baseType = type.nullable();
        } else {
            baseType = type;
        }

        switch (datatype) {
            case STRING:
            case CHARACTER:
                return baseType.stringType().noDefault();
            case BOOLEAN:
                return baseType.booleanType().noDefault();
            case BYTE:
            case SHORT:
            case INTEGER:
                return baseType.intType().noDefault();
            case LONG:
                return baseType.longType().noDefault();
            case FLOAT:
                return baseType.floatType().noDefault();
            case DOUBLE:
                return baseType.doubleType().noDefault();
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                throw new UnsupportedOperationException();
        }
        return fieldAssembler;

    }
}
