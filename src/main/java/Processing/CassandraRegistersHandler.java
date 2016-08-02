package Processing;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import scala.Tuple2;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.SaveMode;

public class CassandraRegistersHandler implements Serializable {

    private final String separator = File.separator;

    DataFrame df;

    public CassandraRegistersHandler(String tableName, String fields, String whereClause, String limit) {

        String query = "";

        if (fields.isEmpty()) {
            fields = "*";
        }

        query = "SELECT " + fields + " FROM " + tableName;

        if (!whereClause.isEmpty()) {
            query += " WHERE " + whereClause;
        }

        if (!limit.isEmpty()) {
            query += " limit " + limit;
        }

        if (SparkConfiguration.sqlContext == null) {
            System.out.println("Spark Context not configured.");
            System.exit(0);
        }

        System.out.println("Querying cassandra ...");
        df = SparkConfiguration.sqlContext.cassandraSql(query);
    }

    public void countRegisters(String columnToFilter) {
        if (!columnToFilter.isEmpty()) {
            df = df.select(columnToFilter).groupBy(columnToFilter).agg(lit(0));
        }
        System.out.println("Number of registers with this query: " + df.count());
    }

    public void saveToLocal(final String idColumnName, final String textColumnName, final String output) {
        df.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row t) throws Exception {
                String id = t.get(t.fieldIndex(idColumnName)).toString();
                String text = t.get(t.fieldIndex(textColumnName)).toString();

                if (text != null) {
                    text = clean(text);

                    if (!text.isEmpty()) {
                        String outputPath = output + separator + id + ".txt";

                        System.out.println(outputPath);

                        Files.write(Paths.get(outputPath), text.getBytes());
                    }
                }

            }
        });
    }

    public void regraPareto(final String textColumnName, final String amountColumnName, final String categoryColumnName, String outputPath, String outputTable) {

        if (SparkConfiguration.sqlContext == null) {
            System.out.println("Spark Conf not configured.");
            System.exit(0);
        }

        JavaPairRDD<String, Tuple2<Double, String>> rdd = df.javaRDD().mapToPair(new PairFunction<Row, String, Tuple2<Double, String>>() {
            @Override
            public Tuple2<String, Tuple2<Double, String>> call(Row row) throws Exception {
                if (row.get(row.fieldIndex(textColumnName)) != null) {
                    String text = row.getAs(row.fieldIndex(textColumnName)).toString();
                    Double value = row.getAs(row.fieldIndex(amountColumnName));
                    String category = row.getAs(row.fieldIndex(categoryColumnName));
                    return new Tuple2<>(text, new Tuple2<>(value, category));
                } else {
                    return new Tuple2<>("", new Tuple2<>(0.0, ""));
                }
            }
        });

        rdd = rdd.filter(new Function<Tuple2<String, Tuple2<Double, String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<Double, String>> t1) throws Exception {
                return t1._1().length() > 0;
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<Double, String>>> rddGrouped = rdd.groupByKey();

        JavaRDD<CountPattern> finalRDD = rddGrouped.map(new Function<Tuple2<String, Iterable<Tuple2<Double, String>>>, CountPattern>() {
            @Override
            public CountPattern call(Tuple2<String, Iterable<Tuple2<Double, String>>> t1) throws Exception {
                String text = t1._1();
                Iterable<Tuple2<Double, String>> values = t1._2();

                Integer count = 0;
                Double value = 0.0;

                Iterator<Tuple2<Double, String>> iterator = values.iterator();

                String category = "";

                while (iterator.hasNext()) {
                    Tuple2<Double, String> t = iterator.next();
                    Double v = t._1();

                    if (category.isEmpty() && !t._2().isEmpty()) {
                        category = t._2();
                    } else if (category.equals("INDEFINIDO")) {
                        category = t._2();
                    }

                    value += v;
                    count++;
                }

                BigDecimal bd = new BigDecimal(value);
                bd = bd.setScale(2, RoundingMode.HALF_UP);

                value = bd.doubleValue();

                return new CountPattern(text, count, value, category);
            }
        });

        DataFrame finalDF = SparkConfiguration.sqlContext.createDataFrame(finalRDD, CountPattern.class);

        finalDF = finalDF.sort(col("count").desc());

        CountPattern cpFinal = finalRDD.reduce(new Function2<CountPattern, CountPattern, CountPattern>() {
            @Override
            public CountPattern call(CountPattern t1, CountPattern t2) throws Exception {
                Integer countSum = t1.getCount() + t2.getCount();
                Double valueSum = t1.getValue() + t2.getValue();

                BigDecimal bd = new BigDecimal(valueSum);
                bd.setScale(2, RoundingMode.HALF_UP);

                valueSum = bd.doubleValue();

                return new CountPattern("", countSum, valueSum, t1.getCategory());
            }
        });

        System.out.println("Sum of count: " + cpFinal.getCount()
                + "\nSum of values: " + cpFinal.getValue());

        if (!outputTable.isEmpty()){
            System.out.println("Saving result to cassandra...");

			Map<String, String> mapOptions = new HashMap<>();
			mapOptions.put("keyspace", SparkConfiguration.sqlContext.getKeyspace());
			mapOptions.put("table", outputTable);

			finalDF.write().format("org.apache.spark.sql.cassandra").options(mapOptions).mode(SaveMode.Append).save();
        }
        if (!outputPath.isEmpty()) {
            finalDF.repartition(1).write().format("com.databricks.spark.csv")
                    .option("delimiter", "|")
                    .option("header", "true")
                    .save(outputPath);
        }
    }

    public void getData(String columnToFilter, String[] filter, String outputPath) {
        df = df.filter(col(columnToFilter).in(filter));

        df.repartition(1).write().format("com.databricks.spark.csv")
                .option("delimiter", "|")
                .option("header", "true")
                .save(outputPath);
    }

    private static String clean(String str) {
        String allowed = "aáãàâbcçdeéêfghiíjklmnoóôõpqrstuúüvwxyz_";
        StringBuilder new_str = new StringBuilder("");
        str = str.toLowerCase();

        for (int i = 0; i < str.length(); i++) {
            String ch = String.valueOf(str.charAt(i));
            if (allowed.contains(ch)) {
                new_str.append(ch);
            } else {
                new_str.append(" ");
            }
        }
        String new_str2 = new_str.toString();
        boolean exit = false;
        int size1 = 0;
        int size2 = 0;

        while (exit == false) {
            size1 = new_str2.length();
            new_str2 = new_str2.replace("  ", " ");
            size2 = new_str2.length();
            if (size1 == size2) {
                exit = true;
            }
        }

        return new_str2.trim();
    }
}
