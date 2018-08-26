package systems.cauldron.drivers.provider;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeProvider {

    protected final URI source;

    protected LakeProvider(URI source) {
        this.source = source;
    }

    public abstract InputStream fetchSource();

    public abstract boolean hasProjectedResults();
}
