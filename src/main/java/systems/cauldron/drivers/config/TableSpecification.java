package systems.cauldron.drivers.config;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class TableSpecification {

    public final String label;
    public final URI location;
    public final FormatSpecification format;
    public final List<ColumnSpecification> columns;

    public TableSpecification(String label, URI location, FormatSpecification format, List<ColumnSpecification> columns) {
        this.label = label;
        this.location = location;
        this.format = format;
        this.columns = columns;
    }

    public TableSpecification(JsonObject object) {
        this.label = object.getString("label");
        this.location = URI.create(object.getString("location"));
        this.format = new FormatSpecification(object.getJsonObject("format"));
        this.columns = object.getJsonArray("columns").stream()
                .map(v -> (JsonObject) v)
                .map(ColumnSpecification::new)
                .collect(Collectors.toList());
    }

    public JsonObject toJson() {
        JsonArrayBuilder columnsJson = Json.createArrayBuilder();
        columns.stream().map(ColumnSpecification::toJson).forEach(columnsJson::add);
        return Json.createObjectBuilder()
                .add("label", label)
                .add("location", location.toString())
                .add("format", format.toJson())
                .add("columns", columnsJson)
                .build();
    }
}
