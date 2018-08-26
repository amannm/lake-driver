package systems.cauldron.drivers.provider;

import systems.cauldron.drivers.config.FormatSpecification;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeGateway {

    protected final URI source;
    protected final FormatSpecification format;
    protected final String query;

    protected LakeGateway(URI source, FormatSpecification format, String query) {
        this.source = source;
        this.format = format;
        this.query = query;
    }

    public abstract InputStream fetchSource();
}
