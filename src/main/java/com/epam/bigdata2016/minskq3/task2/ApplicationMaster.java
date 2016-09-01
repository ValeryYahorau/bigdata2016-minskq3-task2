package com.epam.bigdata2016.minskq3.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ApplicationMaster {

    public int numTotalContainers = 1;
    public Configuration config;

    public void run(String[] args) throws Exception {
        final Path jarPath = new Path("hdfs://sandbox.hortonworks.com:8020/apps/bigdata2016-minskq3-task2-1.2.0-jar-with-dependencies.jar");
        System.out.println("STEP 2 run");
        int n = 1;
        System.out.println("STEP 3 n: " + n);
        Configuration conf = new YarnConfiguration();
        this.config = conf;

        AMRMClient<ContainerRequest> amrmClient = AMRMClient.createAMRMClient();
        amrmClient.init(conf);
        amrmClient.start();
        amrmClient.registerApplicationMaster("", 0, "");

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        for (int i = 0; i < n; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            amrmClient.addContainerRequest(containerAsk);
        }

        Map<String, String> containerEnv = new HashMap<String, String>();
        containerEnv.put("CLASSPATH", "./*");

        int responseId = 0;
        int completedContainers = 0;
        while (completedContainers < n) {
            AllocateResponse response = amrmClient.allocate(responseId++);
            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setEnvironment(containerEnv);
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java" +
                                        " -Xmx256M" +
                                        " com.epam.bigdata2016.minskq3.task2.WordProcessor" +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        )
                );
                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupAppMasterJar(jarPath, appMasterJar);
                ctx.setLocalResources(Collections.singletonMap("bigdata2016-minskq3-task2-1.2.0-jar-with-dependencies.jar", appMasterJar));

                System.out.println("Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
                System.out.println("STEP 4 container started ");
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println("Completed container " + status.getContainerId());
            }
            Thread.sleep(100);
        }








        // Un-register with ResourceManager
        amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        System.out.println("AP STEP X");
        FileStatus jarStat = FileSystem.get(this.config).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("STEP 1 start");
        ApplicationMaster appMaster = new ApplicationMaster();
        appMaster.run(args);
    }
}
