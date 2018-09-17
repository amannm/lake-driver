package systems.cauldron.drivers.lake.adapter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import systems.cauldron.drivers.lake.LakeDriver;
import systems.cauldron.drivers.lake.config.TableSpec;
import systems.cauldron.drivers.lake.scan.AmazonS3URI;
import systems.cauldron.drivers.lake.scan.LakeS3GetScan;
import systems.cauldron.drivers.lake.scan.LakeScan;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class LakeDriverTest {

    private static final String TEST_BUCKET = "build.cauldron.tools";

    @BeforeClass
    public static void setup() {
        stageInputs("people", "relationships");
    }

    @AfterClass
    public static void cleanup() {
        unstageInputs("people", "relationships");
    }


    private static final String TEST_QUERY_A = "select lastname, firstname from people where id = 1337";
    private static final String TEST_RESULT_A = "Malik,Amann";

    private static final String TEST_QUERY_B = "select subject_person_id from relationships where object_person_id = 1337 and predicate_id = 'has_friend'";
    private static final String TEST_RESULT_B = "420";

    private static final String TEST_QUERY_C = "select people.id, relationships.object_person_id from people inner join relationships on people.id = relationships.subject_person_id and relationships.predicate_id = 'has_friend' and people.firstname like '%mann'";
    private static final String TEST_RESULT_C = "1337,420";

//TODO: wait for or figure out S3 Select support on AWS Java SDK v2
//    @Test
//    public void oneTableSimpleFilterS3SelectWhere() throws IOException {
//        String resultString = executeTaskAndGetResult(LakeS3SelectWhereScan.class, generateTableSpecifications("people"), TEST_QUERY_A);
//        assertEquals(TEST_RESULT_A, resultString);
//    }
//
//    @Test
//    public void oneTableComplexFilterS3SelectWhere() throws IOException {
//        String resultString = executeTaskAndGetResult(LakeS3SelectWhereScan.class, generateTableSpecifications("relationships"), TEST_QUERY_B);
//        assertEquals(TEST_RESULT_B, resultString);
//    }
//
//    @Test
//    public void twoTableSimpleJoinS3SelectWhere() throws IOException {
//        String resultString = executeTaskAndGetResult(LakeS3SelectWhereScan.class, generateTableSpecifications("people", "relationships"), TEST_QUERY_C);
//        assertEquals(TEST_RESULT_C, resultString);
//    }
//
//    @Test
//    public void oneTableSimpleFilterS3Select() throws IOException {
//        String resultString = executeTaskAndGetResult(LakeS3SelectScan.class, generateTableSpecifications("people"), TEST_QUERY_A);
//        assertEquals(TEST_RESULT_A, resultString);
//    }
//
//    @Test
//    public void oneTableComplexFilterS3Select() throws IOException {
//        String resultString = executeTaskAndGetResult(LakeS3SelectScan.class, generateTableSpecifications("relationships"), TEST_QUERY_B);
//        assertEquals(TEST_RESULT_B, resultString);
//    }
//
//    @Test
//    public void twoTableSimpleJoinS3Select() throws IOException {
//        String resultString = executeTaskAndGetResult(LakeS3SelectScan.class, generateTableSpecifications("people", "relationships"), TEST_QUERY_C);
//        assertEquals(TEST_RESULT_C, resultString);
//    }

    @Test
    public void oneTableSimpleFilterS3Get() throws IOException {
        String resultString = executeTaskAndGetResult(LakeS3GetScan.class, generateTableSpecifications("people"), TEST_QUERY_A);
        assertEquals(TEST_RESULT_A, resultString);
    }

    @Test
    public void oneTableComplexFilterS3Get() throws IOException {
        String resultString = executeTaskAndGetResult(LakeS3GetScan.class, generateTableSpecifications("relationships"), TEST_QUERY_B);
        assertEquals(TEST_RESULT_B, resultString);
    }

    @Test
    public void twoTableSimpleJoinS3Get() throws IOException {
        String resultString = executeTaskAndGetResult(LakeS3GetScan.class, generateTableSpecifications("people", "relationships"), TEST_QUERY_C);
        assertEquals(TEST_RESULT_C, resultString);
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

    private static List<TableSpec> generateTableSpecifications(String... keys) throws IOException {
        List<TableSpec> builder = new ArrayList<>();
        for (String tableName : keys) {
            Path inputConfig = Paths.get("src", "test", "resources", tableName + ".json");
            try (JsonReader reader = Json.createReader(Files.newBufferedReader(inputConfig, StandardCharsets.UTF_8))) {
                JsonObject jsonObject = reader.readObject();
                TableSpec spec = new TableSpec(jsonObject);
                builder.add(spec);
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
        S3Client s3 = S3Client.builder().build();
        AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        try (InputStream s3is = s3.getObject(GetObjectRequest.builder()
                        .bucket(amazonS3URI.getBucket())
                        .key(amazonS3URI.getKey())
                        .build(),
                ResponseTransformer.toInputStream())) {
            Files.copy(s3is, localDestination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void uploadFile(Path localSource, URI uri) {
        S3Client s3 = S3Client.builder().build();
        AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        s3.putObject(PutObjectRequest.builder()
                        .bucket(amazonS3URI.getBucket())
                        .key(amazonS3URI.getKey())
                        .build(),
                RequestBody.fromFile(localSource));
    }

    public static void deleteFile(URI uri) {
        S3Client s3 = S3Client.builder().build();
        AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        s3.deleteObject(DeleteObjectRequest.builder()
                .bucket(amazonS3URI.getBucket())
                .key(amazonS3URI.getKey())
                .build());
    }
}