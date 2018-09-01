package systems.cauldron.drivers.config;

import javax.json.Json;
import javax.json.JsonObject;

public class ColumnSpecification {

    public final String label;
    public final TypeSpecification datatype;
    public final Boolean nullable;

    public ColumnSpecification(String label, TypeSpecification datatype, Boolean nullable) {
        this.label = label;
        this.datatype = datatype;
        this.nullable = nullable;
    }

    public ColumnSpecification(JsonObject object) {
        this.label = object.getString("label");
        this.datatype = TypeSpecification.of(object.getString("datatype"));
        this.nullable = object.getBoolean("nullable");
    }

    public JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("label", label)
                .add("datatype", datatype.toJson())
                .add("nullable", nullable)
                .build();
    }
}
