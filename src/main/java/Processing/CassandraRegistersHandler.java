package Processing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import scala.Tuple4;

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
                        
                        File file = new File(outputPath);

                        FileWriter fw = new FileWriter(file.getAbsoluteFile());
            			BufferedWriter bw = new BufferedWriter(fw);
            			bw.write(text);
            			bw.close();
                    }
                }

            }
        });
    }

    public void reportPareto(final String textColumnName, final String amountColumnName, final String categoryColumnName, final String financialInstitutionColumnName, final String userIdColumnName, String outputPath, String outputTable, boolean pareto) {

        if (SparkConfiguration.sqlContext == null) {
            System.out.println("Spark Conf not configured.");
            System.exit(0);
        }

        JavaPairRDD<String, Tuple4<Double, String, String, String>> rdd = df.javaRDD().mapToPair(new PairFunction<Row, String, Tuple4<Double, String, String, String>>() {
            @Override
            public Tuple2<String, Tuple4<Double, String, String, String>> call(Row row) throws Exception {
                if (row.get(row.fieldIndex(textColumnName)) != null) {
                    String text = row.getAs(row.fieldIndex(textColumnName)).toString();
                    Double value = row.getAs(row.fieldIndex(amountColumnName));
                    String category = row.getAs(row.fieldIndex(categoryColumnName));
                    String financial_institution = row.getAs(row.fieldIndex(financialInstitutionColumnName));
                    Long client_id = row.getAs(row.fieldIndex(userIdColumnName));
                    
                    financial_institution = financial_institution == null ? "" : financial_institution;
                    
                    return new Tuple2<>(text, new Tuple4<>(value, category, financial_institution, client_id.toString()));
                } else {
                    return new Tuple2<>("", new Tuple4<>(0.0, "", "", ""));
                }
            }
        });

        rdd = rdd.filter(new Function<Tuple2<String, Tuple4<Double, String, String,String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple4<Double, String, String, String>> t1) throws Exception {
                return t1._1().length() > 0;
            }
        });

        JavaPairRDD<String, Iterable<Tuple4<Double, String, String, String>>> rddGrouped = rdd.groupByKey();

        JavaRDD<CountPattern> finalRDD = rddGrouped.map(new Function<Tuple2<String, Iterable<Tuple4<Double, String, String, String>>>, CountPattern>() {
            @Override
            public CountPattern call(Tuple2<String, Iterable<Tuple4<Double, String, String, String>>> t1) throws Exception {
                String text = t1._1().replace(",", "");
                Iterable<Tuple4<Double, String, String, String>> values = t1._2();

                Integer count = 0;
                Double value = 0.0;

                Iterator<Tuple4<Double, String, String, String>> iterator = values.iterator();

                String category = "";
                String prefix = "";
                String financial_institution = "";
                
                List<String> client_ids = new ArrayList<>();
                
                while (iterator.hasNext()) {
                    Tuple4<Double, String, String, String> t = iterator.next();
                    Double v = t._1();

                    if (category.isEmpty() && !t._2().isEmpty()) {
                        category = t._2();
                    } else if (category.equals("INDEFINIDO")) {
                        category = t._2();
                    }

                    if (financial_institution.isEmpty() && !t._3().isEmpty()) {
                    	financial_institution = t._3();
                    } else if (financial_institution.equals("INDEFINIDO")) {
                    	financial_institution = t._3();
                    }
                    
                    
                    if (!client_ids.contains(t._4()))
                    	client_ids.add(t._4());
                    
                    value += v;
                    count++;
                }

                if (!category.isEmpty() && !category.equals("INDEFINIDO"))
                	prefix = category.split("_")[0];
                
                BigDecimal bd = new BigDecimal(value);
                bd = bd.setScale(2, RoundingMode.HALF_UP);

                value = bd.doubleValue();

                String client_ids_ = "";
                
                for (String id : client_ids){
                	if (client_ids_.isEmpty()){
                		client_ids_ = id;
                	} else {
                		client_ids_ += "," + id;
                	}
                }
                
                return new CountPattern(text, count, value, category, prefix, 
                		financial_institution, client_ids_, client_ids.size());
            }
        });

        Double valueTotal = 0.0;
        DataFrame finalDF = null;
        
        if (pareto){
            valueTotal = finalRDD.reduce(new Function2<CountPattern,CountPattern,CountPattern>() {
                @Override
                public CountPattern call(CountPattern t1, CountPattern t2) throws Exception {
                    CountPattern res = new CountPattern(t1.getText(), t1.getCount() + t2.getCount(), t1.getValue() + t2.getValue(), "", "", "", "", t1.getCount_clients() + t2.getCount_clients());
                    
                    return res;
                }
            }).getValue();
            
            Double nro_reg_20perc = finalRDD.count() * 0.2;
            
            finalDF = SparkConfiguration.sqlContext.createDataFrame(finalRDD, CountPattern.class);
            finalDF = finalDF.sort(col("value").desc()).limit(nro_reg_20perc.intValue());
            
            Double v_ = finalDF.javaRDD().map(new Function<Row, Double>() {
                @Override
                public Double call(Row t1) throws Exception {
                    return t1.getAs("value");
                }
            }).reduce(new Function2<Double,Double,Double>() {
                @Override
                public Double call(Double t1, Double t2) throws Exception {
                    return t1 + t2;
                }
            });

            System.out.println("Total: " + valueTotal + "\nValor de 20% dos dados: " + v_);
        }
        else {
            finalDF = SparkConfiguration.sqlContext.createDataFrame(finalRDD, CountPattern.class);
        }

        if (!outputTable.isEmpty()){
            System.out.println("Saving result to cassandra...");

			Map<String, String> mapOptions = new HashMap<>();
			mapOptions.put("keyspace", SparkConfiguration.sqlContext.getKeyspace());
			mapOptions.put("table", outputTable);

			finalDF.write().format("org.apache.spark.sql.cassandra").options(mapOptions).mode(SaveMode.Append).save();
        }
        /*if (!outputPath.isEmpty()) {
            finalDF.repartition(1).write().format("com.databricks.spark.csv")
                    .option("delimiter", "|")
                    .option("header", "true")
                    .save(outputPath);
        }*/
    }

    public void getData(String columnToFilter, String[] filter, String outputTable, String outputPath) {
        df = df.filter(col(columnToFilter).in(filter));

        if (!outputTable.isEmpty()){
            System.out.println("Saving result to cassandra...");

			Map<String, String> mapOptions = new HashMap<>();
			mapOptions.put("keyspace", SparkConfiguration.sqlContext.getKeyspace());
			mapOptions.put("table", outputTable);

			df.write().format("org.apache.spark.sql.cassandra").options(mapOptions).mode(SaveMode.Append).save();
        }
        
        /*df.repartition(1).write().format("com.databricks.spark.csv")
                .option("delimiter", "|")
                .option("header", "true")
                .save(outputPath);*/
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
