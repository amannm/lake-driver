package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.config.TypeSpecification;

import java.util.List;

public interface LakeProviderFactory {

    LakeProvider build(List<RexNode> filters, int[] projects, TypeSpecification[] fieldTypes);


}
