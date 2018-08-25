package systems.cauldron.function.build;

import systems.cauldron.function.build.adapter.LakeSchemaFactory;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class LakeDriver {

    //TODO: make this user exposed interface better and do things better here
    static {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection getConnection(JsonArray tableSpecifications) throws SQLException {

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

    public static void process(JsonArray tableSpecifications, String sqlScript, Path destination) {

        //TODO: don't load all results into memory before writing to disk
        List<String> records = new ArrayList<>();
        try (Connection connection = getConnection(tableSpecifications)) {
            try (PreparedStatement statement = connection.prepareStatement(sqlScript)) {
                ResultSetMetaData metaData = statement.getMetaData();
                int limit = metaData.getColumnCount();
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        List<String> builder = new ArrayList<>();
                        for (int i = 1; i <= limit; i++) {
                            String string = resultSet.getString(i);
                            builder.add(string);
                        }
                        records.add(String.join(",", builder));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            Files.write(destination, records);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
