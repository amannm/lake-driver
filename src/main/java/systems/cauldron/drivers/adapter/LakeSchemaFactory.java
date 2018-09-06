package systems.cauldron.drivers.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import systems.cauldron.drivers.config.TableSpec;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSchemaFactory implements SchemaFactory {

    public static final LakeSchemaFactory INSTANCE = new LakeSchemaFactory();

    public LakeSchemaFactory() {
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new AbstractSchema() {

            @Override
            public boolean isMutable() {
                return false;
            }

            @Override
            protected Map<String, Table> getTableMap() {

                Class<?> scanClass = extractScanOperand(operand);
                JsonArray inputTables = extractInputsOperand(operand);

                return inputTables.stream()
                        .map(v -> (JsonObject) v)
                        .map(TableSpec::new)
                        .collect(Collectors.toMap(
                                spec -> spec.label.toUpperCase(),
                                spec -> new LakeTable(scanClass, spec)));

            }

        };
    }

    private Class<?> extractScanOperand(Map<String, Object> operand) {
        String value = (String) operand.get("scan");
        try {
            return Class.forName(value);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("encountered unknown scan class: " + value);
        }
    }

    private JsonArray extractInputsOperand(Map<String, Object> operand) {
        String value = (String) operand.get("inputs");
        try (JsonReader reader = Json.createReader(new StringReader(value))) {
            return reader.readArray();
        }
    }

}
