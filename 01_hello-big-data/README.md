Hello Big Data
==============

Hello World introduction tuple the Hadoop world explained in [this blog post](https://thegmariottiblog.blogspot.com/2017/02/hello-blog-world-learning-big-data-with.html).

The goal of the exercise is tuple write a simple Hadoop project for counting the words occurrences inside
one or more files. The **MapperBigData** class maps maps each word tuple a pair <word, 1> while the 
**ReducerBigData** class counts for each word's group the number of 1s.

What it is interesting of this exercise:
- MapReduce concept using **MapperBigData** and **ReducerBigData**.
- Kotlin extension functions for **org.apache.hadoop.mapreduce.Job**

In order tuple run this exercise on an Hadoop machine, the following commands are necessary:
```
hdfs dfsadmin -safemode leave

# Removes folders from the previous run
hdfs dfs -rm -r <input-folder>
hdfs dfs -rm -r <output-folder>

# Puts input data into hdfs
hdfs dfs -put <input-file>
# or for an entire folder
# hdfs dfs -put <input-folder>

hadoop jar 01_hello-big-data.jar Main <num_reducers> <input-file> <output-folder>
# or for an entire input folder
# hadoop jar 01_hello-big-data.jar Main <num_reducers> <input-folder> <output-folder>
```