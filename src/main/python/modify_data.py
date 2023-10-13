from py4j.java_collections import ListConverter
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType


class PythonServer:
    def __init__(self, java_gateway, spark):
        self.java_gateway = java_gateway
        self.java_server = java_gateway.entry_point  # Get the Java entry point
        self.spark = spark

    def convert_to_python_rdd(self, java_data):
        # Convert the java list to a python RDD
        python_rdd = self.spark.sparkContext.parallelize(java_data)
        return python_rdd


    def datasets(self):
        # Get the Java Dataset as a list of JSON strings
        java_dataset = self.java_server.getDataset()
        # Convert the Java Dataset to a Python RDD
        python_rdd = self.convert_to_python_rdd(java_dataset.collect())
        print(python_rdd.collect())

        # Perform operations on the Python RDD
        new_string = "NewString"
        updated_python_rdd = python_rdd.union(self.spark.sparkContext.parallelize([new_string]))
        print(updated_python_rdd.collect())  # Display contents
        java_list = ListConverter().convert(updated_python_rdd.collect(), gateway._gateway_client)
        self.java_server.setDataset(java_list)
        # checking if the dataset is updatedd or not.
        updated_java_list = self.java_server.getDataset()
        check_py_rdd = self.spark.sparkContext.parallelize(updated_java_list.collect())
        print(check_py_rdd.collect())

    def process_other_data(self):
        # Process other data (e.g., a list of strings) on the executor nodes
        results = []

        # Get the "other_data" as a list of strings from Java
        other_data = self.java_server.getOtherData()

        # Convert the list of strings to a Java list
        java_list = ListConverter().convert(other_data, gateway._gateway_client)

        # Modify the data, e.g., add " (Modified)"
        for item in java_list:
            results.append(item + " (Modified)")

        # Convert the modified data (Python list) to a Java list
        modified_java_list = ListConverter().convert(results, gateway._gateway_client)

        # Set the modified data using setOtherData method
        self.java_server.setOtherData(modified_java_list)
        print(modified_java_list)


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("PythonServer").getOrCreate()

    # Start a Py4J gateway to connect to Java
    gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_field=True))
    py_server = PythonServer(gateway, spark)

    #py_server.datasets()
    py_server.process_other_data()

    # Shutdown the Py4J gateway
    gateway.shutdown()
    print("Server closed")

    # Stop the Spark session
    spark.stop()