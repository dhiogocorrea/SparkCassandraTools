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

    public void countRegisters(final String columnToFilter, final String amountColumnName, final String idColumnName, final String categoryColumnName, String outputTable) {
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
        	
            JavaPairRDD<String,Tuple4<Integer,Double,String, String>> rdd = df.javaRDD().mapToPair(new PairFunction<Row, String, Tuple4<Integer,Double,String,String>>(){

				@Override
				public Tuple2<String, Tuple4<Integer,Double,String,String>> call(Row t) throws Exception {
					Object obj = t.get(t.fieldIndex(columnToFilter));
					String text = obj.toString();
					
					Double amount = t.getAs(t.fieldIndex(amountColumnName));
					
					Integer row_id = t.getAs(t.fieldIndex(idColumnName));
					
					String category = t.getAs(t.fieldIndex(categoryColumnName));
					text = clean(text);
					
					return new Tuple2<>(text, new Tuple4<>(1,amount,row_id.toString(), category));
				}
            	
            });
            
            JavaPairRDD<String, Tuple4<Integer, Double, String, String>> reducedRDD = rdd.reduceByKey(new Function2<Tuple4<Integer,Double,String, String>,Tuple4<Integer,Double,String, String>,Tuple4<Integer,Double,String, String>>(){

				@Override
				public Tuple4<Integer, Double, String, String> call(Tuple4<Integer, Double, String, String> v1,
						Tuple4<Integer, Double, String, String> v2) throws Exception {
					
					return new Tuple4<>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + "|" + v2._3(), v1._4());
				}
            	
            });
            
            final long total_count_unicos = reducedRDD.count();

            System.out.println("Total de registros unicos: " + total_count_unicos);
            
            
            JavaRDD<SegLinPattern> resultRDD = reducedRDD.map(new Function<Tuple2<String, Tuple4<Integer,Double,String, String>>, SegLinPattern>(){

				@Override
				public SegLinPattern call(Tuple2<String, Tuple4<Integer, Double, String, String>> v1) throws Exception {
					
					Integer count = v1._2()._1();
					Double amount = v1._2()._2();
					
					Double percentual_count = count * 1.0 / total_count;
					Double percentual_amount = amount * 1.0 / total_amount;
					
					BigDecimal bd1 = new BigDecimal(percentual_count);
					bd1.setScale(2, RoundingMode.HALF_UP);
					
					BigDecimal bd2 = new BigDecimal(percentual_amount);
					bd2.setScale(2, RoundingMode.HALF_UP);
					
					return new SegLinPattern(v1._1(), v1._2()._1(), v1._2()._2(), v1._2()._3(), v1._2()._4(), bd1.doubleValue(), bd2.doubleValue());
				}
            	
            });
            
            DataFrame finalDF = SparkConfiguration.sqlContext.createDataFrame(resultRDD, SegLinPattern.class);
            
            
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
