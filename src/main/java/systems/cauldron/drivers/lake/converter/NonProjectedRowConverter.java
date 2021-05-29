package systems.cauldron.drivers.lake.converter;

import systems.cauldron.drivers.lake.config.TypeSpec;

public class NonProjectedRowConverter extends RowConverter {

    private final TypeSpec[] fieldTypes;
    private final int[] projects;

    public NonProjectedRowConverter(TypeSpec[] fieldTypes, int[] projects) {
        this.fieldTypes = fieldTypes;
        this.projects = projects;
    }

    @Override
    public Object[] convertRow(String[] values) {
        final Object[] result = new Object[projects.length];
        for (int i = 0; i < projects.length; i++) {
            int columnIndex = projects[i];
            String value = values[columnIndex];
            if (value != null) {
                TypeSpec type = fieldTypes[columnIndex];
                result[i] = convertField(type, value);
            }
        }
        return result;
    }

}
