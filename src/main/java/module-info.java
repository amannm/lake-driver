module systems.cauldron.drivers.lake {
    exports systems.cauldron.drivers.lake;
    requires slf4j.api;
    requires slf4j.simple;
    requires java.sql;
    requires calcite.core;
    requires calcite.linq4j;
    requires java.json;
    requires univocity.parsers;
    requires sdk.core;
    requires s3;
}