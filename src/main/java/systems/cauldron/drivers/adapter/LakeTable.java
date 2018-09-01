package systems.cauldron.drivers.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.ColumnSpec;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TableSpec;
import systems.cauldron.drivers.config.TypeSpec;
import systems.cauldron.drivers.provider.LakeScan;
import systems.cauldron.drivers.provider.LakeScanner;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;


public class LakeTable extends AbstractTable implements ProjectableFilterableTable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTable.class);

    private final String label;
    private final ColumnSpec[] columns;
    private final FormatSpec format;
    private final LakeScanner scanner;
    private final TypeSpec[] types;
    private final int[] defaultProjects;

    public LakeTable(Class<?> scanClass, TableSpec spec) {
        this.label = spec.label.toUpperCase();
        this.columns = spec.columns.toArray(new ColumnSpec[0]);
        this.format = spec.format;
        this.scanner = LakeScanner.create(scanClass, spec);
        this.types = spec.columns.stream().map(c -> c.datatype).toArray(TypeSpec[]::new);
        this.defaultProjects = IntStream.range(0, columns.length).toArray();
    }

    public String getLabel() {
        return label;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSpec c : columns) {
            RelDataType relDataType = c.datatype.toType(typeFactory);
            builder.add(c.label.toUpperCase(), relDataType);
            builder.nullable(c.nullable == null || c.nullable);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        //TODO: what does it _really_ mean when Calcite passes null projects here?
        projects = (projects == null) ? defaultProjects : projects;

        final LakeScan scan = scanner.scan(types, projects, filters);

        return new AbstractEnumerable<>() {
            public Enumerator<Object[]> enumerator() {
                return new LakeTableEnumerator(format, cancelFlag, scan);
            }
        };
    }

}