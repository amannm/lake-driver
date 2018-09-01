package systems.cauldron.drivers.adapter;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class LakeSchema extends AbstractSchema {

    private Map<String, Table> tableMap;

    public LakeSchema(Map<String, Table> tableMap) {
        super();
        this.tableMap = tableMap;
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
