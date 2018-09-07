package systems.cauldron.drivers.adapter;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import systems.cauldron.drivers.config.ColumnSpec;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TableSpec;
import systems.cauldron.drivers.converter.RowConverter;
import systems.cauldron.drivers.scan.LakeScan;
import systems.cauldron.drivers.scan.LakeScanner;

import java.util.List;
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
            RelDataType relDataType = c.datatype.toType(typeFactory);
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

                RowConverter converter = scan.getRowConverter();
                CsvParser parser = createParser(scan.getFormat());
                parser.beginParsing(scan.getSource());

                return new Enumerator<Object[]>() {

                    private Object[] current;

                    public Object[] current() {
                        return current;
                    }

                    public boolean moveNext() {
                        if (cancelFlag.get()) {
                            return false;
                        }
                        final String[] strings = parser.parseNext();
                        if (strings == null) {
                            current = null;
                            parser.stopParsing();
                            return false;
                        }
                        current = converter.convertRow(strings);
                        return true;
                    }

                    public void reset() {
                        throw new UnsupportedOperationException();
                    }

                    public void close() {
                        if (!parser.getContext().isStopped()) {
                            parser.stopParsing();
                        }
                    }

                };
            }

        };
    }

    private static CsvParser createParser(FormatSpec spec) {

        CsvFormat format = new CsvFormat();
        format.setDelimiter(spec.delimiter);
        format.setLineSeparator(spec.lineSeparator);
        format.setQuote(spec.quoteChar);
        format.setQuoteEscape(spec.escape);

        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(format);
        parserSettings.setHeaderExtractionEnabled(spec.header);

        return new CsvParser(parserSettings);

    }

}