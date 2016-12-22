###Information

1) First we set up our pom.xml file with the Hbase dependencies, and the build and plugins dependencies. The pom.xml file is available in the folder.

2) We get the hbase-site.xml file from the cluster with the following scp command :
<code><pre> scp user_id@cluster_adress:/etc/hbase/conf/hbase-site.xml /C/TP_HBase/ </pre></code>

Then, we put the hbase_site.xml configuration file in the following folder of our repository, as below
<code><pre> /C/TP_HBase/conf/hbase-site.xml </pre></code>

3) We write the Java code to create the Hbase table, which is able to design a social network. 






