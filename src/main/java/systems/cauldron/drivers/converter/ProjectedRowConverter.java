package systems.cauldron.drivers.converter;

import systems.cauldron.drivers.config.TypeSpec;

public class ProjectedRowConverter extends RowConverter {

    private final TypeSpec[] projectedFieldTypes;

    public ProjectedRowConverter(TypeSpec[] projectedFieldTypes) {
        this.projectedFieldTypes = projectedFieldTypes;
    }

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
