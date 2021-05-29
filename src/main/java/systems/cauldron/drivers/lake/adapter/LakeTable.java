package systems.cauldron.drivers.lake.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import systems.cauldron.drivers.lake.config.ColumnSpec;
import systems.cauldron.drivers.lake.config.TableSpec;
import systems.cauldron.drivers.lake.parser.CsvInputStreamParser;
import systems.cauldron.drivers.lake.scan.LakeScan;
import systems.cauldron.drivers.lake.scan.LakeScanner;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;


public class LakeTable extends AbstractTable implements ProjectableFilterableTable {

    private final ColumnSpec[] columns;
    private final LakeScanner scanner;
    private final int[] defaultProjects;

    LakeTable(Class<?> scanClass, TableSpec spec) {
        this.columns = spec.columns.toArray(new ColumnSpec[0]);
        this.scanner = LakeScanner.create(scanClass, spec);
        this.defaultProjects = IntStream.range(0, columns.length).toArray();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSpec c : columns) {
            RelDataType relDataType = typeFactory.createJavaType(c.datatype.toJavaClass());
            builder.add(c.label.toUpperCase(), relDataType);
            builder.nullable(c.nullable == null || c.nullable);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        //TODO: understand how Calcite works here and optimize

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        projects = (projects == null) ? defaultProjects : projects;
        final LakeScan scan = scanner.apply(projects, filters);

        return new AbstractEnumerable<>() {

            public Enumerator<Object[]> enumerator() {

                CsvInputStreamParser parser = new CsvInputStreamParser(
                        scan.getFormat(),
                        scan.getRowConverter(),
                        scan.getSource());

                return new Enumerator<>() {

                    private Object[] current;

                    public Object[] current() {
                        return current;
                    }

                    public boolean moveNext() {
                        if (cancelFlag.get()) {
                            return false;
                        }
                        Optional<Object[]> result = parser.parseRecord();
                        if (result.isEmpty()) {
                            current = null;
                            return false;
                        }
                        current = result.get();
                        return true;
                    }

                    public void reset() {
                        throw new UnsupportedOperationException();
                    }

                    public void close() {
                        parser.close();
                    }

                };
            }

        };
    }

}