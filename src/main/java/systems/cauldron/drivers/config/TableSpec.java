package systems.cauldron.drivers.config;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class TableSpec {

    public final String label;
    public final URI location;
    public final FormatSpec format;
    public final List<ColumnSpec> columns;

    public TableSpec(String label, URI location, FormatSpec format, List<ColumnSpec> columns) {
        this.label = label;
        this.location = location;
        this.format = format;
        this.columns = columns;
    }

    public TableSpec(JsonObject object) {
        this.label = object.getString("label");
        this.location = URI.create(object.getString("location"));
        this.format = new FormatSpec(object.getJsonObject("format"));
        this.columns = object.getJsonArray("columns").stream()
                .map(v -> (JsonObject) v)
                .map(ColumnSpec::new)
                .collect(Collectors.toList());
    }

    public JsonObject toJson() {
        JsonArrayBuilder columnsJson = Json.createArrayBuilder();
        columns.stream().map(ColumnSpec::toJson).forEach(columnsJson::add);
        return Json.createObjectBuilder()
                .add("label", label)
                .add("location", location.toString())
                .add("format", format.toJson())
                .add("columns", columnsJson)
                .build();
    }
}
