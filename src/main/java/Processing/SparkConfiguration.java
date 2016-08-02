package Processing;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;


public abstract class SparkConfiguration implements Serializable{

    public transient static JavaSparkContext sc;
    public static CassandraSQLContext sqlContext;
    
    public static void configureContext(String jarLocation, String sparkHost, String appName, String cassandraHost, String cassandraUsername,
            String cassandraPassword, String keyspace) {
        System.out.println("Configuring Context ...");

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", cassandraHost)
                .set("spark.cassandra.auth.username", cassandraUsername)
                .set("spark.cassandra.auth.password", cassandraPassword);

        String[] jars = new String[2];
        jars[0] = jarLocation;
        jars[1] = "/home/dcorrea/jars/spark-csv_2.10-1.4.0.jar";
        Class[] classes = new Class[3];
        classes[0] = CassandraRegistersHandler.class;
        classes[1] = CountPattern.class;
        classes[2] = SparkConfiguration.class;
         
        conf.registerKryoClasses(classes);
        
        conf.setJars(jars);
        
        sc = new JavaSparkContext(sparkHost, appName, conf);

        sqlContext = new CassandraSQLContext(sc.sc());
        sqlContext.setKeyspace(keyspace);
    }
}
