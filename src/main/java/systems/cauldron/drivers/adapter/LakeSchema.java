package systems.cauldron.drivers.adapter;

import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSchema extends AbstractSchema {

    private Map<String, Table> tableMap;
    private Map<String, RelProtoDataType> typeMap;

    public LakeSchema(List<LakeTable> tables) {
        super();
        this.tableMap = tables.stream().collect(Collectors.toMap(LakeTable::getLabel, t -> t));
        this.typeMap = tableMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> (RelProtoDataType) relDataTypeFactory -> e.getValue().getRowType(relDataTypeFactory)));
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    @Override
    protected Map<String, RelProtoDataType> getTypeMap() {
        return typeMap;
    }
}
