package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.FormatSpecification;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

public abstract class LakeProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LakeProvider.class);

    protected final URI source;
    protected final FormatSpecification format;
    protected final String query;

    protected LakeProvider(URI source, FormatSpecification format, List<RexNode> filters, int[] projects) {
        this.source = source;
        this.format = format;
        this.query = compileQuery(filters, projects);
        LOG.info("{}", query);
    }

    protected abstract String compileQuery(List<RexNode> filters, int[] projects);

    public abstract InputStream fetchSource();


}
