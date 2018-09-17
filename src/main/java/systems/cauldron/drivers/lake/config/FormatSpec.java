package systems.cauldron.drivers.lake.config;

import javax.json.Json;
import javax.json.JsonObject;

public class FormatSpec {

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

    public FormatSpec(char delimiter, String lineSeparator, char quoteChar, char escape, char commentChar, boolean header, boolean strictQuotes, boolean ignoreLeadingWhiteSpace, boolean ignoreQuotations, NullFieldIndicator nullFieldIndicator, CompressionType compression) {
        this.delimiter = delimiter;
        this.lineSeparator = lineSeparator;
        this.quoteChar = quoteChar;
        this.escape = escape;
        this.commentChar = commentChar;
        this.header = header;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
        this.ignoreQuotations = ignoreQuotations;
        this.nullFieldIndicator = nullFieldIndicator;
        this.compression = compression;
    }

    FormatSpec(JsonObject object) {
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

    JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("delimiter", "" + delimiter)
                .add("lineSeparator", lineSeparator)
                .add("quoteChar", "" + quoteChar)
                .add("escape", "" + escape)
                .add("commentChar", "" + commentChar)
                .add("header", header)
                .add("strictQuotes", strictQuotes)
                .add("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
                .add("ignoreQuotations", ignoreQuotations)
                .add("nullFieldIndicator", nullFieldIndicator.name().toLowerCase())
                .add("compression", compression.name().toLowerCase())
                .build();

    }

}
