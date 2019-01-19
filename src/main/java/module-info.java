module systems.cauldron.drivers.lake {
    exports systems.cauldron.drivers.lake;
    requires slf4j.api;
    requires slf4j.simple;
    requires java.sql;
    requires java.json;
    requires calcite.core;
    requires calcite.linq4j;
    requires univocity.parsers;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.services.s3;
}