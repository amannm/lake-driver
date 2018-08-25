package systems.cauldron.drivers.config;

import javax.json.JsonObject;

public class FormatSpecification {

    public final char delimiter;
    public final String lineSeparator;
    public final char quoteChar;
    public final char escape;
    public final char commentChar;
    public final boolean header;
    public final boolean strictQuotes;
    public final boolean ignoreLeadingWhiteSpace;
    public final boolean ignoreQuotations;
    public final NullFieldIndicator nullFieldIndicator;
    public final CompressionType compression;

    public FormatSpecification(JsonObject object) {
        this.delimiter = object.getString("delimiter").charAt(0);
        this.lineSeparator = object.getString("lineSeparator");
        this.quoteChar = object.getString("quoteChar").charAt(0);
        this.escape = object.getString("escape").charAt(0);
        this.commentChar = object.getString("commentChar").charAt(0);
        this.header = object.getBoolean("header");
        this.strictQuotes = object.getBoolean("strictQuotes");
        this.ignoreLeadingWhiteSpace = object.getBoolean("ignoreLeadingWhiteSpace");
        this.ignoreQuotations = object.getBoolean("ignoreQuotations");
        this.nullFieldIndicator = NullFieldIndicator.valueOf(object.getString("nullFieldIndicator").toUpperCase());
        this.compression = CompressionType.valueOf(object.getString("compression").toUpperCase());
    }

    public enum CompressionType {
        NONE,
        GZIP
    }

    public enum NullFieldIndicator {
        EMPTY_SEPARATORS,
        EMPTY_QUOTES,
        BOTH,
        NEITHER
    }

}
