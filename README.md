# bigdata2016-minskq3-task2
Task2: Yarn Application


###STEP 1 
Delete jar from hdfs if you are copied jar to "/apps" folder before
```
hadoop fs -rm -skipTrash /apps/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar
```


###STEP 2 
Build project
```
mvn clean install
```


###STEP 3 
Copy jar to hdfs
```
/usr/hdp/2.4.0.0-169/hadoop/bin/hadoop fs -copyFromLocal ~/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar /apps/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar
```

e.g. 

/usr/hdp/2.4.0.0-169/hadoop/bin/hadoop fs -copyFromLocal /root/Documents/bigdata2016-minskq3-task2/target/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar /apps/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar


###STEP 4 
Run Yarn Application
```
yarn jar ~/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task2.Client ~/user.profile.tags.us.txt	2	hdfs://sandbox.hortonworks.com:8020/apps/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar
```

e.g. 

yarn jar /root/Documents/bigdata2016-minskq3-task2/target/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task2.Client /tmp/admin/user.profile.tags.us.min3.txt	2	hdfs://sandbox.hortonworks.com:8020/apps/bigdata2016-minskq3-task2-1.0.3-jar-with-dependencies.jar


###STEP 5
Results
Check folder /tmp/admin/ and find out result fiels there.
