package systems.cauldron.drivers;

import systems.cauldron.drivers.adapter.LakeSchemaFactory;
import systems.cauldron.drivers.config.TableSpecification;
import systems.cauldron.drivers.provider.LakeProvider;
import systems.cauldron.drivers.provider.LakeS3GetProvider;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class LakeDriver implements Driver {

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


    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
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

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
