package systems.cauldron.drivers.converter;

import systems.cauldron.drivers.config.TypeSpec;

public class SimpleRowConverter extends RowConverter {

    private final TypeSpec[] fieldTypes;

    public SimpleRowConverter(TypeSpec[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public Object[] convertRow(String[] values) {
        final Object[] result = new Object[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            String value = values[i];
            if (value != null) {
                TypeSpec type = fieldTypes[i];
                result[i] = convertField(type, value);
            }
        }
        return result;
    }
}
