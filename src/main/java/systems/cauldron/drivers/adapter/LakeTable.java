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
import systems.cauldron.drivers.provider.LakeS3SelectFilterTranslator;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;


public class LakeTable extends AbstractTable implements ProjectableFilterableTable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTable.class);

    private final String label;
    private final URI source;
    private final List<ColumnSpecification> columns;
    private final FormatSpecification format;

    public LakeTable(TableSpecification specification) {
        this.label = specification.getLabel().toUpperCase();
        this.source = specification.getLocation();
        this.columns = specification.getColumns();
        this.format = specification.getFormat();
    }

    public String getLabel() {
        return label;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSpecification c : columns) {
            RelDataType relDataType = LakeFieldType.of(c.getDatatype()).toType(typeFactory);
            builder.add(c.getLabel().toUpperCase(), relDataType);
            builder.nullable(c.isNullable());
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        projects = projects == null ? IntStream.range(0, columns.size()).toArray() : projects;
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        final String s3SelectQuery = LakeS3SelectFilterTranslator.compileS3SelectQuery(filters, projects);
        final LakeFieldType[] fieldTypes = generateFieldList(projects);

        LOG.info("{}", s3SelectQuery);

        return new AbstractEnumerable<>() {
            public Enumerator<Object[]> enumerator() {
                return new LakeTableEnumerator(source, format, s3SelectQuery, fieldTypes, cancelFlag);
            }
        };
    }

    private LakeFieldType[] generateFieldList(int[] fields) {
        return IntStream.of(fields).boxed()
                .map(columns::get)
                .map(ColumnSpecification::getDatatype)
                .map(LakeFieldType::of)
                .toArray(LakeFieldType[]::new);
    }


}