package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import py4j.GatewayServer;

import java.util.*;

public class Main {
    private ArrayList<String> new_list = new ArrayList<>();
    private transient SparkSession spark;
    private transient JavaSparkContext sc;
    private Dataset<String> dataset;
    private ArrayList<String> otherData = new ArrayList<>();

    public Dataset<String> getDataset() {
        // Initialize JavaSparkContext
        if (sc == null) {
            spark = SparkSession.builder().master("local").appName("samplePy4j").getOrCreate();
            sc = new JavaSparkContext(spark.sparkContext());
        }
        Dataset<String> dataset = spark.createDataset(new_list, Encoders.STRING());

        return dataset;
    }
    public void setDataset(ArrayList<String> input_list){
        if (sc == null) {
            spark = SparkSession.builder().master("local").appName("samplePy4j").getOrCreate();
            sc = new JavaSparkContext(spark.sparkContext());
        }
        new_list=input_list;
    }

    public void setlist(ArrayList<String> new_list) {
        this.new_list = new_list;
    }

    public List<String> getlist() {
        return new_list;
    }
    public void setOtherData(ArrayList<String> otherData) {
        this.otherData = otherData;
    }

    public ArrayList<String> getOtherData() {
        return otherData;
    }

    public static void main(String[] args) {
        Main obj = new Main();
        obj.setlist(new ArrayList<>(Arrays.asList("Hi", "Hello", "Hey")));
        obj.setOtherData(new ArrayList<>(Arrays.asList("exec1","exec2","exec3")));

        GatewayServer gatewayServer = new GatewayServer(obj);
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}