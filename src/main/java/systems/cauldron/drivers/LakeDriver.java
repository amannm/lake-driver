package systems.cauldron.drivers;

import systems.cauldron.drivers.adapter.LakeSchemaFactory;
import systems.cauldron.drivers.config.TableSpecification;
import systems.cauldron.drivers.provider.LakeProvider;
import systems.cauldron.drivers.provider.LakeS3GetProvider;

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

    public static Connection getConnection(List<TableSpecification> tables) throws SQLException {
        return getConnection(tables, LakeS3GetProvider.class);
    }

    public static Connection getConnection(List<TableSpecification> tables, Class<? extends LakeProvider> providerClass) throws SQLException {

        JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
        tables.stream().map(TableSpecification::toJson).forEach(jsonArrayBuilder::add);
        JsonArray build = jsonArrayBuilder.build();
        String tableSpecificationsString = build.toString();

        String schemaFactoryName = LakeSchemaFactory.class.getName();
        String providerName = providerClass.getName();

        JsonObject modelJson = Json.createObjectBuilder()
                .add("version", "1.0")
                .add("defaultSchema", "default")
                .add("schemas", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("name", "default")
                                .add("type", "custom")
                                .add("factory", schemaFactoryName)
                                .add("operand", Json.createObjectBuilder()
                                        .add("provider", providerName)
                                        .add("inputTables", tableSpecificationsString))))
                .build();

        return DriverManager.getConnection("jdbc:calcite:model=inline:" + modelJson);
    }

}
