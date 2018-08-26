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
import systems.cauldron.drivers.config.ColumnSpecification;
import systems.cauldron.drivers.config.FormatSpecification;
import systems.cauldron.drivers.config.TableSpecification;
import systems.cauldron.drivers.provider.LakeProvider;
import systems.cauldron.drivers.provider.LakeProviderFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;


public class LakeTable extends AbstractTable implements ProjectableFilterableTable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTable.class);

    private final String label;
    private final List<ColumnSpecification> columns;
    private final FormatSpecification format;
    private final LakeProviderFactory providerFactory;

    public LakeTable(TableSpecification specification, Class<?> lakeProviderClass) {
        this.label = specification.label.toUpperCase();
        this.columns = specification.columns;
        this.format = specification.format;
        this.providerFactory = LakeProviderFactory.create(lakeProviderClass, specification);
    }

    public String getLabel() {
        return label;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSpecification c : columns) {
            RelDataType relDataType = LakeFieldType.of(c.datatype).toType(typeFactory);
            builder.add(c.label.toUpperCase(), relDataType);
            builder.nullable(c.nullable == null || c.nullable);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        final LakeFieldType[] allFieldTypes = columns.stream()
                .map(c -> c.datatype)
                .map(LakeFieldType::of)
                .toArray(LakeFieldType[]::new);
        projects = projects == null ? IntStream.range(0, columns.size()).toArray() : projects;
        LakeProvider provider = providerFactory.build(filters, projects, allFieldTypes);
        return new AbstractEnumerable<>() {
            public Enumerator<Object[]> enumerator() {
                return new LakeTableEnumerator(format, cancelFlag, provider);
            }
        };
    }


}