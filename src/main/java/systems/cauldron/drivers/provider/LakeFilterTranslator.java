package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;

import java.util.List;

public interface LakeFilterTranslator {
    String compileQuery(List<RexNode> filters, int[] projects);
}
