Sensors Analysis
============

Sensors Analysis exercise using Hadoop and Kotlin, explained in [Work In Progess]()

The goal of this exercise is to analyze sensors generated data. The data can be in two different formats, while 
the output is differentiate between sensors reading over or under a certain temperature threshold.

What it is interesting of this exercise:
- Use of **SensorsDriver** for setting up multiple inputs and outputs.
- Use of **SensorData** as a way of representing the reading of a sensor.
- Use of **SensorsMapperType1** and **SensorsMapperType2** for handling different sensors inputs.

In order to run this exercise on an Hadoop machine, the following commands are necessary:
```
hdfs dfsadmin -safemode leave

# Removes folders from the previous run
hdfs dfs -rm -r <input-folder>
hdfs dfs -rm -r <output-folder>

# Puts input data into hdfs
hdfs dfs -put <input-folder>

# <input-type1> and <input-type2> can be a folder or a file
hadoop jar 03_sensors-analysis.jar Main <input-folder>/<input-type1> <input-folder>/<input-type2> <output-folder> <threshold>
```