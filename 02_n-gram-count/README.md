N-Gram Count
============

N-Gram Count exercise using Hadoop and Kotlin, explained in [this blog post](http://thegmariottiblog.blogspot.com/2017/02/combiners-counters-and-properties-in.html)

The goal of this exercise is to count n-words at a time, with n coming from the terminal, 
filtering out n-grams that start with a user defined keyword, counting how many words have been
removed.

What it is interesting of this exercise:
- Use of **NGramCombiner** for reducing network operations.
- Use of **Properties** and **Counters** for sharing data with Mapper, Reducer and Combiner.

In order to run this exercise on an Hadoop machine, the following commands are necessary:
```
hdfs dfsadmin -safemode leave

# Removes folders from the previous run
hdfs dfs -rm -r <input-folder>
hdfs dfs -rm -r <output-folder>

# Puts input data into hdfs
hdfs dfs -put <input-file>
# or for an entire folder
# hdfs dfs -put <input-folder>

hadoop jar 02_n-gram-count.jar Main <num_reducers> <input-file> <output-folder> <n> <keyword>
# or for an entire input folder
# hadoop jar 01_hello-big-data.jar Main <num_reducers> <input-folder> <output-folder> <n> <keyword>
```