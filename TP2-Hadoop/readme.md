# TP2 Hadoop

### Information

+ I used Intellij and Maven to create and compile the project, with Java 1.8 installed
+ I generated a single jar file containing the three mapreduce functions, which I uploaded on the virtual machine with the scp command.
+ I ran the queries on the jar file with the hadoop jar command

### Useful commands
Upload Jar to the server
<pre><code> scp filepath.jar login@MV_IP </code></pre>
Run Java queries from the Jar with Hadoop
<pre><code>hadoop jar filename.jar javaquery filetoparse.csv outputfile </code></pre>
View output 
<pre><code>hdfs dfs -cat outputfile/part-r-00000</code></pre>



### Questions 

1) Create M/R Softwares to get these stats (1 M/R per stat) :
- Count first name by origin
- Count number of first names by number of origins (how many first names have x origins ? For x = 1,2,3...)
-  Proportion (in%) of male or female

2) For each M/R, can we use combiner? Why?

### Answers : 

1) I created the java code for the two first stats. They are attached in the folder. 
For the third stat, I was not able to compute the frequency. I was only able to compute the number of occurences of male and female names, and the total number of names. The total number of names is also given at the end of the Hadoop mapreduce job, since the MAP_INPUT_RECORDS is the same as the number of names. Please see below. I think we could solve this using the below code but I was not able to use it.

<pre><code>job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue()</code></pre>

        Map-Reduce Framework
                Map input records=11526
                Map output records=23485
                Map output bytes=233118
                Map output materialized bytes=38
                Input split bytes=119
                Combine input records=23485
                Combine output records=3
                Reduce input groups=3
                Reduce shuffle bytes=38
                Reduce input records=3
                Reduce output records=3
                Spilled Records=6
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=67
                CPU time spent (ms)=2620
                Physical memory (bytes) snapshot=1966940160
                Virtual memory (bytes) snapshot=10612101120
                Total committed heap usage (bytes)=2013790208


2) For the two first M/R queries, we are computing an addition of word counts. Therefore, it is fine to use a combiner as the sum is both commutative and associative as it creates no issues between the map and reduce phases.
For the third M/R, we are computing a proportion of word counts (male and female occurences in the list). Here, we should not use a combiner as the proportio computation is not neither commutative nor associative.

