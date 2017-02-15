Hello Big Data
==============

Hello World introduction to the Hadoop world explained in [this](https://thegmariottiblog.blogspot.com/2017/02/hello-blog-world-learning-big-data-with.html) blog post.

The goal of the exercise is to write a simple Hadoop project for counting the words occurrences inside
one or more files. The **MapperBigData** class maps maps each word to a pair <word, 1> while the 
**ReducerBigData** class counts for each word's group the number of 1s.

What is interesting of this exercise:
- MapReduce concept using **MapperBigData** and **ReducerBigData**.
- Kotlin extension functions for **org.apache.hadoop.mapreduce.Job**
