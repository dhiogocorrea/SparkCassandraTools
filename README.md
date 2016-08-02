# Spark Cassandra Tools
<h3>A set of tools to manipulate cassandra using spark</h3>
This project was created to help users to make things that CQLSH is unable to do in large ammounts of data.
<br>
<br>
For now, there are only two proccess available. Feel free to make some more!
<hr>
<b>1. Count number of rows from a query. For this one, insert the follow arguments:</b>

typeProcessing:countRegisters sparkHost:\<your_spark_host_name\> appName:\<your_app_name\> cassandraHost:\<your_cassandra_host\> cassandraUsername:\<your_cassandra_username\> cassandraPassword:\<your_cassandra_password\> keyspace:\<your_keyspace\> tableName:\<name_of_table_to_count\> whereClause:\<where_clause\> limit:\<limit_number\>

<i>The whereClause and limit arguments are opcional.</i>

<b>2. Save content from a column to local file, cleanning the text:</b>

typeProcessing:saveToLocal sparkHost:\<your_spark_host_name\> appName:\<your_app_name\> cassandraHost:\<your_cassandra_host\> cassandraUsername:\<your_cassandra_username\> cassandraPassword:\<your_cassandra_password\> keyspace:\<your_keyspace\> tableName:\<name_of_table_to_count\> idColumnName:\<name_of_id_column\> textColumnName:\<name_of_column_that_will_be_saved\> output:\<output_path\> whereClause:\<where_clause\> limit:\<limit_number\>

<i>The whereClause and limit arguments are opcional.</i>

<hr>

Powered by <a href="http://www.itera.com.br">Itera</a>
