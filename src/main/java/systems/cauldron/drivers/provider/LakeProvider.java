package systems.cauldron.drivers.provider;

import systems.cauldron.drivers.config.FormatSpecification;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeProvider {


    protected final URI source;
    protected final FormatSpecification format;

    protected LakeProvider(URI source, FormatSpecification format) {
        this.source = source;
        this.format = format;
    }

    public abstract InputStream fetchSource();

}
