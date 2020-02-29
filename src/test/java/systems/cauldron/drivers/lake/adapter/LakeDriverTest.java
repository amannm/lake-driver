package systems.cauldron.drivers.lake.adapter;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import systems.cauldron.drivers.lake.LakeDriver;
import systems.cauldron.drivers.lake.config.TableSpec;
import systems.cauldron.drivers.lake.scan.LakeS3GetScan;
import systems.cauldron.drivers.lake.scan.LakeS3SelectScan;
import systems.cauldron.drivers.lake.scan.LakeS3SelectWhereScan;
import systems.cauldron.drivers.lake.scan.LakeScan;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LakeDriverTest {

    private static final String TEST_BUCKET = "build.cauldron.tools";

    @BeforeAll
    public static void setup() {
        stageInputs("people", "relationships");
    }

    @AfterAll
    public static void cleanup() {
        unstageInputs("people", "relationships");
    }

    private static Stream<Arguments> inputProvider() {
        return Stream.of(
                Arguments.of(
                        "select lastname, firstname from people where id = 1337",
                        generateTableSpecifications("people"),
                        "Malik,Amann"
                ),
                Arguments.of(
                        "select subject_person_id from relationships where object_person_id = 1337 and predicate_id = 'has_friend'",
                        generateTableSpecifications("relationships"),
                        "420"
                ),
                Arguments.of(
                        "select people.id, relationships.object_person_id from people inner join relationships on people.id = relationships.subject_person_id and relationships.predicate_id = 'has_friend' and people.firstname like '%mann'",
                        generateTableSpecifications("people", "relationships"),
                        "1337,420"
                ),
                Arguments.of(
                        "select distinct people.id from people inner join relationships on people.id = relationships.subject_person_id and (relationships.predicate_id = 'has_friend' or relationships.predicate_id = 'has_enemy')",
                        generateTableSpecifications("people", "relationships"),
                        "420\n69\n1337"
                ));
    }


    @ParameterizedTest
    @MethodSource("inputProvider")
    void testS3SelectWhere(String query, List<TableSpec> schema, String result) throws IOException {
        String resultString = executeTaskAndGetResult(LakeS3SelectWhereScan.class, schema, query);
        assertEquals(result, resultString);
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void testS3Select(String query, List<TableSpec> schema, String result) throws IOException {
        String resultString = executeTaskAndGetResult(LakeS3SelectScan.class, schema, query);
        assertEquals(result, resultString);
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void testS3Get(String query, List<TableSpec> schema, String result) throws IOException {
        String resultString = executeTaskAndGetResult(LakeS3GetScan.class, schema, query);
        assertEquals(result, resultString);
    }


    private static String executeTaskAndGetResult(Class<? extends LakeScan> clazz, List<TableSpec> tables, String sqlScript) throws IOException {
        Path localResult = Files.createTempFile(null, null);
        queryToFile(clazz, tables, sqlScript, localResult);
        String result = Files.lines(localResult, StandardCharsets.UTF_8).collect(Collectors.joining("\n"));
        Files.delete(localResult);
        return result;
    }

    private static void queryToFile(Class<? extends LakeScan> clazz, List<TableSpec> tableSpecs, String sqlScript, Path destination) {

        List<String> records = new ArrayList<>();
        try (Connection connection = LakeDriver.getConnection(tableSpecs, clazz)) {
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

    private static List<TableSpec> generateTableSpecifications(String... keys) {
        List<TableSpec> builder = new ArrayList<>();
        for (String tableName : keys) {
            Path inputConfig = Paths.get("src", "test", "resources", tableName + ".json");
            try (JsonReader reader = Json.createReader(Files.newBufferedReader(inputConfig, StandardCharsets.UTF_8))) {
                JsonObject jsonObject = reader.readObject();
                TableSpec spec = new TableSpec(jsonObject);
                builder.add(spec);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return builder;
    }

    private static void stageInputs(String... keys) {
        for (String tableName : keys) {
            Path inputBuild = Paths.get("src", "test", "resources", tableName + ".csv");
            uploadFile(inputBuild, URI.create("s3://" + TEST_BUCKET + "/" + tableName));
        }
    }

    private static void unstageInputs(String... keys) {
        for (String tableName : keys) {
            deleteFile(URI.create("s3://" + TEST_BUCKET + "/" + tableName));
        }
    }

    public static void downloadFile(URI uri, Path localDestination) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final AmazonS3URI sourceUri = new AmazonS3URI(uri);
        try {
            S3Object o = s3.getObject(sourceUri.getBucket(), sourceUri.getKey());
            try (S3ObjectInputStream s3is = o.getObjectContent()) {
                Files.copy(s3is, localDestination, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (AmazonServiceException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void uploadFile(Path localSource, URI uri) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final AmazonS3URI destinationUri = new AmazonS3URI(uri);
        try {
            s3.putObject(destinationUri.getBucket(), destinationUri.getKey(), localSource.toFile());
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteFile(URI uri) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final AmazonS3URI destinationUri = new AmazonS3URI(uri);
        try {
            s3.deleteObject(destinationUri.getBucket(), destinationUri.getKey());
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }
    }
}