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
import static org.apache.spark.sql.functions.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.SaveMode;
import scala.Tuple5;

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

    public void countRegisters(final String columnToFilter, final String amountColumnName, final String idColumnName, final String categoryColumnName, final String categoryPercentualColumnName, String outputTable) {
        if (!columnToFilter.isEmpty()) {
            //df = df.groupBy(columnToFilter).agg(lit(0));
            
        	df = df.filter(col(idColumnName).isNotNull()).filter(col(amountColumnName).isNotNull()).filter(col(columnToFilter).isNotNull());
                
        	Tuple4<Integer, Double, Integer, Double> total = df.javaRDD().map(new Function<Row, Tuple4<Integer,Double,Integer, Double>>(){

				@Override
				public Tuple4<Integer, Double, Integer, Double> call(Row t) throws Exception {
					Double amount = t.getAs(t.fieldIndex(amountColumnName));
					String category = t.getAs(t.fieldIndex(categoryColumnName));
                                        
					Integer isIndefinido = category.equals("INDEFINIDO") ? 1 : 0;
					
                                        Double amount_indefinido = category.equals("INDEFINIDO") ? amount : 0;
                                        
					return new Tuple4<>(1, amount, isIndefinido, amount_indefinido);
				}}).reduce(new Function2<Tuple4<Integer,Double,Integer, Double>,Tuple4<Integer,Double, Integer, Double>,Tuple4<Integer,Double, Integer, Double>>(){

					@Override
					public Tuple4<Integer, Double, Integer,Double> call(Tuple4<Integer, Double, Integer, Double> v1, Tuple4<Integer, Double, Integer,Double> v2)
							throws Exception {
						Double value = v1._2() + v2._2();
						
						BigDecimal bd = new BigDecimal(value);
						bd.setScale(2, RoundingMode.HALF_UP);
						
						Integer count = v1._1() + v2._1();
										
                                                Double value_indefinido = v1._4() + v2._4();
						
						BigDecimal bd2 = new BigDecimal(value_indefinido);
						bd2.setScale(2, RoundingMode.HALF_UP);
                                                
						return new Tuple4<>(count, bd.doubleValue(), v1._3() + v2._3(), bd2.doubleValue());
					}});
        	
        	final Integer total_count = total._1();
        	final Double total_amount = total._2();
        	final Integer total_indefinidos = total._3();
        	final Double total_amount_indefinidos = total._4();
                
        	System.out.println("Total de registros: " + total_count +
        			"\nTotal em valor: " + total_amount + 
        			"\nTotal de indefinidos: " + total_indefinidos +
                                "\nTotal em valor dos indefinidos: " + total_amount_indefinidos +
                                "\nTotal de registros categorizados: " + (total_count - total_indefinidos) +
                                "\nTotal em valor de registros categorizados: " + (total_amount - total_amount_indefinidos));
        	
            JavaPairRDD<String,Tuple5<Integer,Double,String, String,Integer>> rdd = df.javaRDD().mapToPair(new PairFunction<Row, String, Tuple5<Integer,Double,String, String,Integer>>(){

				@Override
				public Tuple2<String, Tuple5<Integer,Double,String, String,Integer>> call(Row t) throws Exception {
					Object obj = t.get(t.fieldIndex(columnToFilter));
					String text = obj.toString();
					
					Double amount = t.getAs(t.fieldIndex(amountColumnName));
					
					Integer row_id = t.getAs(t.fieldIndex(idColumnName));
					
					String category = t.getAs(t.fieldIndex(categoryColumnName));
					text = clean(text);
					
                                        Double category_percentual = t.getAs(t.fieldIndex(categoryPercentualColumnName));
                                        
                                        Integer category_percentual_quartil = 0;
                                        
                                        if (category_percentual > 0 && category_percentual <= 25)
                                            category_percentual_quartil = 1;
                                        else if (category_percentual > 25 && category_percentual <= 50)
                                            category_percentual_quartil = 2;
                                        else if (category_percentual > 50 && category_percentual <= 75)
                                            category_percentual_quartil = 3;
                                        else if (category_percentual > 75 && category_percentual <= 100)
                                            category_percentual_quartil = 4;
                                        
					return new Tuple2<>(text, new Tuple5<>(1,amount,row_id.toString(), category,category_percentual_quartil));
				}
            	
            });
            
            JavaPairRDD<String, Tuple5<Integer,Double,String, String,Integer>> reducedRDD = rdd.reduceByKey(new Function2<Tuple5<Integer,Double,String, String,Integer>,Tuple5<Integer,Double,String, String,Integer>,Tuple5<Integer,Double,String, String,Integer>>(){

				@Override
				public Tuple5<Integer,Double,String, String,Integer> call(Tuple5<Integer,Double,String, String,Integer> v1,
						Tuple5<Integer,Double,String, String,Integer> v2) throws Exception {
					
					return new Tuple5<>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + "|" + v2._3(), v1._4(), v1._5());
				}
            	
            });
            
            final long total_count_unicos = reducedRDD.count();

            System.out.println("Total de registros unicos: " + total_count_unicos);
            
            
            JavaRDD<SegLinPattern> resultRDD = reducedRDD.map(new Function<Tuple2<String, Tuple5<Integer,Double,String, String,Integer>>, SegLinPattern>(){

				@Override
				public SegLinPattern call(Tuple2<String, Tuple5<Integer,Double,String, String,Integer>> v1) throws Exception {
					String text = v1._1();
                                        
					Integer count = v1._2()._1();
					Double amount = v1._2()._2();
					
					Double percentual_count = count * 1.0 / total_count;
					Double percentual_amount = amount * 1.0 / total_amount;
					
					BigDecimal bd1 = new BigDecimal(percentual_count);
					bd1.setScale(2, RoundingMode.HALF_UP);
					
					BigDecimal bd2 = new BigDecimal(percentual_amount);
					bd2.setScale(2, RoundingMode.HALF_UP);
					
                                        if (text.isEmpty()){
                                            return new SegLinPattern("", v1._2()._1(), v1._2()._2(), v1._2()._3(), v1._2()._4(), bd1.doubleValue(), bd2.doubleValue(), v1._2()._5());
                                        } else {
                                            return new SegLinPattern(v1._1(), v1._2()._1(), v1._2()._2(), v1._2()._3(), v1._2()._4(), bd1.doubleValue(), bd2.doubleValue(), v1._2()._5());
                                        }
					
				}
            	
            });
            
            DataFrame finalDF = SparkConfiguration.sqlContext.createDataFrame(resultRDD, SegLinPattern.class);
            
            finalDF = finalDF.filter(col("rsegda_lin_extrt").notEqual(""));
            
            finalDF.show();
            
            if (!outputTable.isEmpty()){
                System.out.println("Saving result to cassandra...");

    			Map<String, String> mapOptions = new HashMap<>();
    			mapOptions.put("keyspace", SparkConfiguration.sqlContext.getKeyspace());
    			mapOptions.put("table", outputTable);

    			finalDF.write().format("org.apache.spark.sql.cassandra").options(mapOptions).mode(SaveMode.Append).save();
                        
                        Double num20perc_double = total_count_unicos * 0.2;
                        
                        
                        BigDecimal bd = new BigDecimal(num20perc_double);
                        bd.setScale(1, RoundingMode.UP);
                        
                        
                        
                        Double d = Math.ceil(bd.doubleValue());
                        
                        
                        Integer num20_perc = d.intValue();
                        
                        DataFrame df_amount = finalDF.orderBy(desc("percentual_amount"))
                                .limit(num20_perc).withColumn("pareto_amount", lit(1));
                        
                        DataFrame df_count = finalDF.orderBy(desc("percentual_count"))
                                .limit(num20_perc).withColumn("pareto_count", lit(1));

                        df_amount.write().format("org.apache.spark.sql.cassandra").options(mapOptions).mode(SaveMode.Append).save();
                        df_count.write().format("org.apache.spark.sql.cassandra").options(mapOptions).mode(SaveMode.Append).save();
                        
            }
            
        }
        else {
        	System.out.println("Number of registers with this query: " + df.count());
        }
        
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

        df = df.filter(col(userIdColumnName).isNotNull());
    }
    
    public void getData(String columnToFilter, String filter_type, String[] filter, String outputTable, String outputPath, String textColumnName) {
        
        if (filter_type.equals("in"))
            df = df.filter(col(columnToFilter).in(filter));
        else if (filter_type.equals("greater")){
            Double filterDouble = Double.parseDouble(filter[0]);
            
            df = df.filter(col(columnToFilter).gt(filterDouble));
        } else if (filter_type.equals("between")){
            Double filterDouble1 = Double.parseDouble(filter[0]);
            Double filterDouble2 = Double.parseDouble(filter[1]);
            
            df = df.filter(col(columnToFilter).between(filterDouble1, filterDouble2));
        }

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


