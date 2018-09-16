module systems.cauldron.drivers.lake {

    //SQL engine
    requires java.sql;
    requires calcite.core;
    requires calcite.linq4j;

    //JSON tools
    requires java.json;

    //flat-file parsing
    requires univocity.parsers;

    //cloud storage
    requires aws.java.sdk.s3;

}