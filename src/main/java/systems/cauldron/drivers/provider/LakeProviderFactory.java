package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.adapter.LakeFieldType;

import java.util.List;

public interface LakeProviderFactory {
    LakeProvider getProvider(List<RexNode> filters, int[] projects, LakeFieldType[] fieldTypes);
}
