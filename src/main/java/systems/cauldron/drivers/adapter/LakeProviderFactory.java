package systems.cauldron.drivers.adapter;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.provider.LakeProvider;

import java.util.List;

public interface LakeProviderFactory {
    LakeProvider getProvider(List<RexNode> filters, int[] projects, LakeFieldType[] fieldTypes);
}
