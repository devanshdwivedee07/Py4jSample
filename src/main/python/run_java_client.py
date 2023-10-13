import subprocess

# Path of the Java file
java_file = "/Users/devanshdwivedee/PycharmProjects/Py4J12OCT/src/main/java/Main.java"

# Classpath of the Py4J JAR file
classpath = "/Users/devanshdwivedee/PycharmProjects/Py4J12OCT/src/main/java/lib/py4j-0.10.9.7.jar:."

# Command to run the Java class
java_command = f"java -cp {classpath} {java_file}"

try:
    # Run the Java class as a subprocess
    subprocess.check_call(java_command, shell=True)
except subprocess.CalledProcessError as e:
    print(f"Error running Java client: {e}")
