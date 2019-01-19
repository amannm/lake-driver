package systems.cauldron.drivers.lake.scan;

import systems.cauldron.drivers.lake.config.TypeSpec;
import systems.cauldron.drivers.lake.parser.RecordParser;

public abstract class LakeScan {

    final TypeSpec[] types;
    final int[] projects;

    LakeScan(TypeSpec[] types, int[] projects) {
        this.types = types;
        this.projects = projects;
    }

    public abstract RecordParser getRecordParser();

}
