###Information

* I used Intellij and Maven to create and compile the project, with Java 1.8 installed
* I generated a single jar file containing the Hbase function. This jar file is an "uber-jar" with the maven-shade plugin included. This allows to dispatch some useful classes on the cluster.
* We can choose to run the jar directly on the cluster via the hadoop jar command, or we can also use directly Intellij to populate the social network

###Steps

1) First we set up our pom.xml file with the Hbase dependencies, and the build and plugins dependencies. The pom.xml file is available in the folder. I used the below link to set up the pom.xml file. 
http://www.informit.com/articles/article.aspx?p=2255108&seqNum=2

2) We get the hbase-site.xml file from the cluster with the following scp command :
<code><pre> scp user_id@cluster_adress:/etc/hbase/conf/hbase-site.xml /C/TP_HBase/ </pre></code>

Then, we put the hbase_site.xml configuration file in the following folder of our repository, as below
<code><pre> /C/TP_HBase/conf/hbase-site.xml </pre></code>

I used IntelliJ with Maven to do this work on Hbase.

3) We write the Java code to create the Hbase table, which is able to design a social network. The Java code is available in this folder.
I found some help here:
https://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/

4) We build the project with Intellij, and we generate the Jar file with the "package" maven function

5) We upload it to the cluster via the below command 
<code><pre>  scp filepath.jar login@MV_IP </pre></code>

Note : Before uploading it to the cluster, I had to reduce the size of the Jar file as the space on the cluster was limited. I excluded some useless functions that were included in the Jar file via the Pom.xml file and the maven-shade plugin. Please see the pom.xml file code for more information. I found some help here : 
https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html

### Social Network

The Java program creates a social network using Hbase and a REPL (Read-Eval-Print-Loop) to fill up the database

The database contains the following info :
as row id: the firstnames of peoples
as column families : info, friends
in info: gender, age
in friends: list of friends and BFF : name of the Best Friend (row ID)

Below is an excerpt of the database : 

Current Social Network info :
Jack friends:BFF 1482770594675 Anna
Jack friends:others 1482770604475 Chloé,René
Jack info:age 1482770576545 34
Jack info:gender 1482770577522 M
Jennifer friends:BFF 1482770922063 Madonna
Jennifer friends:others 1482770934895 Nciolas,Anna,
Jennifer info:age 1482770909248 22
Jennifer info:gender 1482770910524 M


### Useful Hbase commands

To enter Hbase on the cluster
<code><pre>  hbase shell </pre></code>

To list the existing tables
<code><pre>  list </pre></code>

To delete a table (2 steps)
<code><pre>  disable 't1' </pre></code>
<code><pre>  drop 't1' </pre></code>

To view more information on a table
<code><pre>  describe 't1' </pre></code>









