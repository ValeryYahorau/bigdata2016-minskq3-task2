package com.epam.bigdata2016.minskq3.task2;

import org.apache.hadoop.fs.Path;

public class Constants {
    public static final String HDFS_ROOT_PATH = "hdfs://sandbox.hortonworks.com:8020";
    public static final Path HDFS_MY_APP_JAR_PATH = new Path(Constants.HDFS_ROOT_PATH + "/apps/bigdata2016-minskq3-task2-1.0.0-jar-with-dependencies.jar");
    public static final String JAR_NAME = "bigdata2016-minskq3-task2-1.0.1-jar-with-dependencies.jar";
}

