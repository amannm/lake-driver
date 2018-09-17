package systems.cauldron.drivers.lake;

import systems.cauldron.drivers.lake.adapter.LakeSchemaFactory;
import systems.cauldron.drivers.lake.config.TableSpec;
import systems.cauldron.drivers.lake.scan.LakeS3GetScan;
import systems.cauldron.drivers.lake.scan.LakeScan;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class LakeDriver {

    static {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection(List<TableSpec> tables) throws SQLException {
        return getConnection(tables, LakeS3GetScan.class);
    }

    public static Connection getConnection(List<TableSpec> tables, Class<? extends LakeScan> scanClass) throws SQLException {

        JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
        tables.stream().map(TableSpec::toJson).forEach(jsonArrayBuilder::add);
        JsonArray build = jsonArrayBuilder.build();
        String tableSpecificationsString = build.toString();

        String schemaFactoryName = LakeSchemaFactory.class.getName();
        String scanClassName = scanClass.getName();

        JsonObject modelJson = Json.createObjectBuilder()
                .add("version", "1.0")
                .add("defaultSchema", "default")
                .add("schemas", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("name", "default")
                                .add("type", "custom")
                                .add("factory", schemaFactoryName)
                                .add("operand", Json.createObjectBuilder()
                                        .add("scan", scanClassName)
                                        .add("inputs", tableSpecificationsString))))
                .build();

        return DriverManager.getConnection("jdbc:calcite:model=inline:" + modelJson);
    }

}
