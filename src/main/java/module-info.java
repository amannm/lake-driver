module systems.cauldron.drivers.lake {

    //SQL engine
    requires java.sql;
    requires calcite.core;
    requires calcite.linq4j;

    //JSON tools
    requires java.json;

    //flat-file parsing
    requires univocity.parsers;

    //compression
    //requires parquet.common;
    //requires parquet.hadoop;
    //requires avro;
    //requires parquet.avro;

    //logging
    //requires slf4j.api;
    //requires slf4j.simple;

    //cloud storage
    requires aws.java.sdk.s3;

}