package systems.cauldron.drivers;

import systems.cauldron.drivers.adapter.LakeSchemaFactory;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class LakeDriver {

    static {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection(JsonArray tableSpecifications) throws SQLException {

        String tableSpecificationsString = tableSpecifications.toString();

        String schemaFactoryName = LakeSchemaFactory.class.getName();

        JsonObject modelJson = Json.createObjectBuilder()
                .add("version", "1.0")
                .add("defaultSchema", "default")
                .add("schemas", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("name", "default")
                                .add("type", "custom")
                                .add("factory", schemaFactoryName)
                                .add("operand", Json.createObjectBuilder()
                                        .add("inputTables", tableSpecificationsString))))
                .build();

        return DriverManager.getConnection("jdbc:calcite:model=inline:" + modelJson);
    }

}
