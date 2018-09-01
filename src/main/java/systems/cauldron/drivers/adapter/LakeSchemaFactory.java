package systems.cauldron.drivers.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.TableSpec;
import systems.cauldron.drivers.provider.LakeScanner;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSchemaFactory implements SchemaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSchemaFactory.class);

    public static final LakeSchemaFactory INSTANCE = new LakeSchemaFactory();

    public LakeSchemaFactory() {
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

        Class<?> scanClass = extractScanOperand(operand);

        JsonArray inputTables = extractInputTablesOperand(operand);

        Map<String, Table> tableMap = inputTables.stream()
                .map(v -> (JsonObject) v)
                .map(TableSpec::new)
                .collect(Collectors.toMap(
                        spec -> spec.label.toUpperCase(),
                        spec -> {
                            LakeScanner scanner = LakeScanner.create(scanClass, spec);
                            return new LakeTable(scanner, spec);
                        }));

        return new LakeSchema(tableMap);
    }

    private Class<?> extractScanOperand(Map<String, Object> operand) {
        String value = (String) operand.get("scan");
        try {
            return Class.forName(value);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("encountered unknown scan class: " + value);
        }
    }

    private JsonArray extractInputTablesOperand(Map<String, Object> operand) {
        String value = (String) operand.get("inputs");
        try (JsonReader reader = Json.createReader(new StringReader(value))) {
            return reader.readArray();
        }
    }

}
