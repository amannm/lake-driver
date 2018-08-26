package systems.cauldron.drivers.adapter;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSchema extends AbstractSchema {

    private Map<String, Table> tableMap;

    public LakeSchema(List<LakeTable> tables) {
        super();
        this.tableMap = tables.stream().collect(Collectors.toMap(LakeTable::getLabel, t -> t));
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }
}
