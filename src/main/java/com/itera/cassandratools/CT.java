package com.itera.cassandratools;

import Processing.CassandraRegistersHandler;
import Processing.SparkConfiguration;

public class CT {

    public static void main(String[] args) {
        long tempoInicio = System.currentTimeMillis();

        if (args.length == 0) {
            System.exit(0);
        }

        String jarLocation = "/home/dcorrea/jars/SCT.jar";

        String sparkHost = "yarn-client";
        String appName = "SCTAPP";
        String cassandraHost = "10.244.203.23";
        String cassandraUsername = "bradesco";
        String cassandraPassword = "brades01";
        String keyspace = "itera_miner";

        String tableName = "event_cc";
        String fields = "";
        String whereClause = "";
        String limit = "";

        String idColumnName = "row_id";
        String textColumnName = "rsegda_lin_extrt";

        String output = "";

        String typeProcessing = "";

        String amountColumnName = "event_amt";

        String categoryColumnName = "category";
        
        String outputTable = "";
        
        String columnToFilter = "";
        String[] filter = null;

        for (String parameter : args) {
            parameter = parameter.replace("\\", "/");
            String[] parameters = parameter.split(":");
            if (parameters.length < 2) {
                System.exit(0);
            }
            switch (parameters[0]) {
                case "jarLocation":
                    if (parameters.length == 3) {
                        jarLocation = parameters[1] + ":" + parameters[2];
                    } else if (parameters.length == 4) {
                        jarLocation = parameters[1] + ":" + parameters[2]
                                + ":" + parameters[3];
                    } else {
                        jarLocation = parameters[1];
                    }
                    break;
                case "sparkHost":
                    if (parameters.length == 3) {
                        sparkHost = parameters[1] + ":" + parameters[2];
                    } else if (parameters.length == 4) {
                        sparkHost = parameters[1] + ":" + parameters[2]
                                + ":" + parameters[3];
                    } else {
                        sparkHost = parameters[1];
                    }
                    break;
                case "appName":
                    if (parameters.length == 3) {
                        appName = parameters[1] + ":" + parameters[2];
                    } else {
                        appName = parameters[1];
                    }
                    break;
                case "cassandraHost":
                    if (parameters.length == 3) {
                        cassandraHost = parameters[1] + ":" + parameters[2];
                    } else {
                        cassandraHost = parameters[1];
                    }
                    break;
                case "cassandraUsername":
                    if (parameters.length == 3) {
                        cassandraUsername = parameters[1] + ":" + parameters[2];
                    } else {
                        cassandraUsername = parameters[1];
                    }
                    break;
                case "cassandraPassword":
                    if (parameters.length == 3) {
                        cassandraPassword = parameters[1] + ":" + parameters[2];
                    } else {
                        cassandraPassword = parameters[1];
                    }
                    break;
                case "keyspace":
                    if (parameters.length == 3) {
                        keyspace = parameters[1] + ":" + parameters[2];
                    } else {
                        keyspace = parameters[1];
                    }
                    break;
                case "tableName":
                    if (parameters.length == 3) {
                        tableName = parameters[1] + ":" + parameters[2];
                    } else {
                        tableName = parameters[1];
                    }
                    break;
                case "fields":
                    if (parameters.length == 3) {
                        fields = parameters[1] + ":" + parameters[2];
                    } else {
                        fields = parameters[1];
                    }
                    break;
                case "whereClause":
                    if (parameters.length == 3) {
                        whereClause = parameters[1] + ":" + parameters[2];
                    } else {
                        whereClause = parameters[1];
                    }
                    break;
                case "typeProcessing":
                    if (parameters.length == 3) {
                        typeProcessing = parameters[1] + ":" + parameters[2];
                    } else {
                        typeProcessing = parameters[1];
                    }
                    break;
                case "limit":
                    if (parameters.length == 3) {
                        limit = parameters[1] + ":" + parameters[2];
                    } else {
                        limit = parameters[1];
                    }
                    break;
                case "output":
                    if (parameters.length == 3) {
                        output = parameters[1] + ":" + parameters[2];
                    } else {
                        output = parameters[1];
                    }
                    break;
                case "idColumnName":
                    if (parameters.length == 3) {
                        idColumnName = parameters[1] + ":" + parameters[2];
                    } else {
                        idColumnName = parameters[1];
                    }
                    break;
                case "textColumnName":
                    if (parameters.length == 3) {
                        textColumnName = parameters[1] + ":" + parameters[2];
                    } else {
                        textColumnName = parameters[1];
                    }
                    break;
                case "amountColumnName":
                    if (parameters.length == 3) {
                        amountColumnName = parameters[1] + ":" + parameters[2];
                    } else {
                        amountColumnName = parameters[1];
                    }
                    break;
                case "columnToFilter":
                    if (parameters.length == 3) {
                        columnToFilter = parameters[1] + ":" + parameters[2];
                    } else {
                        columnToFilter = parameters[1];
                    }
                    break;
                case "filter":
                    filter = parameters[1].split(",");
                    break;
                    case "outputTable":
                    if (parameters.length == 3) {
                        outputTable = parameters[1] + ":" + parameters[2];
                    } else {
                        outputTable = parameters[1];
                    }
                    break;
            }
        }

        SparkConfiguration.configureContext(jarLocation, sparkHost, appName, cassandraHost, cassandraUsername, cassandraPassword, keyspace);
        CassandraRegistersHandler crh = new CassandraRegistersHandler(tableName, fields, whereClause, limit);

        switch (typeProcessing) {
            case "countRegisters":
                crh.countRegisters(columnToFilter);
                break;
            case "saveToLocal":
                crh.saveToLocal(idColumnName, textColumnName, output);
                break;
            case "regraPareto":
        {
            crh.regraPareto(textColumnName, amountColumnName, categoryColumnName, output, outputTable);
        }
                break;
            case "dataInformation":
                crh.getData(columnToFilter, filter, output);
                break;
        }

    }
}
