package systems.cauldron.drivers.lake.converter;

import systems.cauldron.drivers.lake.config.TypeSpec;

public class ProjectedRowConverter extends RowConverter {

    private final TypeSpec[] projectedFieldTypes;

    public ProjectedRowConverter(TypeSpec[] projectedFieldTypes) {
        this.projectedFieldTypes = projectedFieldTypes;
    }

    @Override
    public Object[] convertRow(String[] values) {
        final Object[] result = new Object[projectedFieldTypes.length];
        for (int i = 0; i < projectedFieldTypes.length; i++) {
            String value = values[i];
            if (value != null) {
                TypeSpec type = projectedFieldTypes[i];
                result[i] = convertField(type, value);
            }
        }
        return result;
    }
}
