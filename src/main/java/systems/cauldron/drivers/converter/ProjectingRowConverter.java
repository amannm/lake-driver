package systems.cauldron.drivers.converter;

import systems.cauldron.drivers.config.TypeSpec;

public class ProjectingRowConverter extends RowConverter {

    private final TypeSpec[] allFieldTypes;
    private final int[] projects;

    public ProjectingRowConverter(TypeSpec[] allFieldTypes, int[] projects) {
        this.allFieldTypes = allFieldTypes;
        this.projects = projects;
    }

    public Object[] convertRow(String[] values) {
        final Object[] result = new Object[projects.length];
        for (int i = 0; i < projects.length; i++) {
            int columnIndex = projects[i];
            String value = values[columnIndex];
            if (value != null) {
                TypeSpec type = allFieldTypes[columnIndex];
                result[i] = convertField(type, value);
            }
        }
        return result;
    }

}
