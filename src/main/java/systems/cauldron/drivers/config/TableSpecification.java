package systems.cauldron.drivers.config;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TableSpecification {

    private final String label;
    private final URI location;
    private final FormatSpecification format;
    private final List<ColumnSpecification> columns;

    public TableSpecification(JsonObject object) {
        this.label = object.getString("label");
        this.location = URI.create(object.getString("location"));
        this.format = new FormatSpecification(object.getJsonObject("format"));
        this.columns = object.getJsonArray("columns").stream()
                .map(v -> (JsonObject) v)
                .map(ColumnSpecification::new)
                .collect(Collectors.toList());
    }

    public String getLabel() {
        return label;
    }

    public URI getLocation() {
        return location;
    }

    public FormatSpecification getFormat() {
        return format;
    }

    public List<ColumnSpecification> getColumns() {
        return Collections.unmodifiableList(columns);
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
