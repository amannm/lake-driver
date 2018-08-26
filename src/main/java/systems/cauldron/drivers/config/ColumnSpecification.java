package systems.cauldron.drivers.config;

import javax.json.Json;
import javax.json.JsonObject;

public class ColumnSpecification {

    public final String label;
    public final String datatype;
    public final Boolean nullable;

    public ColumnSpecification(String label, String datatype, Boolean nullable) {
        this.label = label;
        this.datatype = datatype;
        this.nullable = nullable;
    }

    public ColumnSpecification(JsonObject object) {
        this.label = object.getString("label");
        this.datatype = object.getString("datatype");
        this.nullable = object.getBoolean("nullable");
    }

    public JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("label", label)
                .add("datatype", datatype)
                .add("nullable", nullable)
                .build();
    }
}
