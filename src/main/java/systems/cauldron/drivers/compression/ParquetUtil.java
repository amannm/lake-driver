package systems.cauldron.drivers.compression;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import systems.cauldron.drivers.config.ColumnSpec;
import systems.cauldron.drivers.config.TableSpec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;


public class ParquetUtil {

    public static void read(Path source, Consumer<GenericRecord> recordHandler) throws IOException {
        InputFile inputFile = new ParquetInputFile(source);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                recordHandler.accept(record);
            }
        }
    }

    public static void convert(TableSpec tableSpec, Path source, Path destination) throws IOException {
        Schema schema = toAvroSchema(tableSpec);
        List<GenericRecord> records = Collections.emptyList();
        OutputFile outputFile = new ParquetOutputFile(destination);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        }
    }

    public static GenericRecord record(Schema avroSchema, Object[] values) {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (int i = 0; i < values.length; i++) {
            record.put(i, values[i]);
        }
        return record;
    }


    private static Schema toAvroSchema(TableSpec tableSpec) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(tableSpec.label).fields();
        for (ColumnSpec columnSpec : tableSpec.columns) {
            fields = handleBuilder(columnSpec, fields);
        }
        return fields.endRecord();
    }


    private static SchemaBuilder.FieldAssembler<Schema> handleBuilder(ColumnSpec columnSpec, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {

        SchemaBuilder.FieldTypeBuilder<Schema> type = fieldAssembler.name(columnSpec.label).type();

        SchemaBuilder.BaseFieldTypeBuilder<Schema> baseType;
        if (columnSpec.nullable) {
            baseType = type.nullable();
        } else {
            baseType = type;
        }

        switch (columnSpec.datatype) {
            case STRING:
            case CHARACTER:
                return baseType.stringType().noDefault();
            case BOOLEAN:
                return baseType.booleanType().noDefault();
            case BYTE:
            case SHORT:
            case INTEGER:
                return baseType.intType().noDefault();
            case LONG:
                return baseType.longType().noDefault();
            case FLOAT:
                return baseType.floatType().noDefault();
            case DOUBLE:
                return baseType.doubleType().noDefault();
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                throw new UnsupportedOperationException();
        }
        return fieldAssembler;

    }



}