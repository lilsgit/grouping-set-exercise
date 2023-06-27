import org.apache.commons.lang3.tuple.Pair;
import tech.tablesaw.aggregate.AggregateFunction;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReader;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import static tech.tablesaw.aggregate.AggregateFunctions.max;
import static tech.tablesaw.aggregate.AggregateFunctions.sum;

public class tablesaw {
    public static void main(String[] args) {
        Table mainTable = Table.read().csv("src/dataset1.csv");
        Table dictTable = Table.read().csv("src/dataset2.csv");

        Table mergedTable = mainTable.joinOn("counter_party").inner(dictTable);

        Table aggTable = mergedTable.summarize(
                mergedTable.column("rating").max().as("max_rating"),
                mergedTable.intColumn("value").sum().where(mergedTable.stringColumn("status").isEqualTo("ARAP")).as("sum_value_ARAP"),
                mergedTable.intColumn("value").sum().where(mergedTable.stringColumn("status").isEqualTo("ACCR")).as("sum_value_ACCR")
        ).by("legal_entity", "counter_party", "tier");


//                mergedTable
//                .summarize("value","rating", sum,  max)
//                .by("legal_entity", "counter_party", "tier", "status");

        System.out.println(aggTable);
    }
}
