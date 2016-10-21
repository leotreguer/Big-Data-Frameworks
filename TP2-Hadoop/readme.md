##TP2 Hadoop

Information

- I used Intellij and Maven to create and compile the project, with Java 1.8 installed
- I generated a single jar file containing the three mapreduce functions, which I uploaded on the virtual machine with the scp command.
- I ran the queries on the jar file with the hadoop jar command

Questions 

1) Create M/R Softwares to get these stats (1 M/R per stat) :
- Count first name by origin
- Count number of first names by number of origins (how many first names have x origins ? For x = 1,2,3...)
-  Proportion (in%) of male or female

2) For each M/R, can we use combiner? Why?

Answers : 

1) I created the java code for the two first stats. They are attached in the folder. 
For the third stat, I was only able to compute the number of male and female percentages, not the proportion.

2) For the two first M/R queries, we are computing an addition of word counts. Therefore, it is fine to use a combiner as the sum is both commutative and associative as it creates no issues between the map and reduce phases.
For the third M/R, we are computing a proportion of word counts (male and female occurences in the list). Here, we should not use a combiner as the proportio computation is not neither commutative nor associative.

