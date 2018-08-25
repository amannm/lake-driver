package systems.cauldron.function.build.config;

import javax.json.JsonObject;

public class ColumnSpecification {

    private final String label;
    private final String datatype;
    private final Boolean nullable;

    public ColumnSpecification(JsonObject object) {
        this.label = object.getString("label");
        this.datatype = object.getString("datatype");
        this.nullable = object.getBoolean("nullable");
    }

    public String getLabel() {
        return label;
    }

    public String getDatatype() {
        return datatype;
    }

    public boolean isNullable() {
        return nullable == null || nullable;
    }
}
