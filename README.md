# hadoop-mapreduce-examples

Start Hadoop:
$ hadoop-3.2.0/sbin/start-all.sh 
$ hadoop version (Checks the Hadoop Installation and Version) 
$ javac -version (Checks JAVA version need - jdk8)
$ hadoop-3.2.0/sbin/stop-all.sh  


$ export HADOOP_CLASSPATH = $(hadoop classpath) \
$ echo $HADOOP_CLASSPATH (Checks hadoop classpath) \

Hadoop HDFS Helpful Commands: \
$ hdfs dfs -ls \
$ hdfs dfs -mkdir \
$ hdfs dfs -put 'sample.txt' /user/test/sampl.txt \
$ hdfs dfs -cat /user/view*\
$ hdfs dfs -copyFromLocal 'local_directory/path' /hdfs_directory/path/\
$ hdfs dfs -rm -r /hdfs/file/delete \
 
 Map Reduce Steps: 
 1. Create the .java files for Map-Reduce (javafile.java). Create Input folder on HDFS with the input text files. 
 2. Compile to create JAVA Class files: 
 $ javac -classpath ${HADOOP_CLASSPATH} -d 'classes_folder/javaclass' 'javafile.java' 
 3. Create .jar files: 
 $ jar -cvf <JAR FILE NAME> -c '<classes_folder/CLASSNAME>' / . 
 4. Run jar file: 
  hadoop jar 'JAR FILE NAME.jar' CLASS_NAME <HDFS_INPUT_TEXTFILE_PATH> <HDFS_OUTPUT_FILEPATH> 
  or on HDFS Cluster 
  yarn jar jarfile.jar ClassName -libjars /path/to/jar1.jar,/path/to/jar2.jar arg0 arg1 


  
