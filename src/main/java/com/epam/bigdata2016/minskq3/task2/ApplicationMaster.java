package com.epam.bigdata2016.minskq3.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {

    private Configuration configuration;
    private int numContainers;
    private int splits;
    private int totalItemsCount;
    private AtomicInteger count = new AtomicInteger(0);
    private String inputPath;
    private NMClient nmClient;

    public ApplicationMaster(String inputPath, int n) {

        this.inputPath = inputPath;
        this.numContainers = n;
        this.splits = numContainers;

        initTotalItemsToProcess();

        configuration = new YarnConfiguration();
    }


    private void initTotalItemsToProcess() {
        totalItemsCount = 0;
        try {
            totalItemsCount = HDFSHelper.countLines(new Path(Constants.HDFS_ROOT_PATH + inputPath));
            System.out.println("ApplicationMaster. Total items count: " + totalItemsCount);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        final String inputPath = args[0];
        final int n = Integer.valueOf(args[1]);
        //final String inputPath = "/tmp/admin/user.profile.tags.us.min3.txt";
        //final int n = 2;
        ApplicationMaster appMaster = new ApplicationMaster(inputPath, n);
        appMaster.run();
    }

    public void run() throws Exception {
        AMRMClientAsync<ContainerRequest> amrmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        amrmClient.init(getConfiguration());
        amrmClient.start();
        RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster("", 0, "");

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();

        for (int i = 0; i < numContainers; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            ;
            amrmClient.addContainerRequest(containerAsk);
        }

        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    public boolean doneWithContainers() {
        return numContainers == 0;
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public void onShutdownRequest() {
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            synchronized (this) {
                numContainers--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            System.out.println("[AM] onContainersAllocated count: " + count.incrementAndGet());

            int[] numbersSegment = Utils.getNumbersSegment(totalItemsCount, splits, count.intValue());

            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java" +
                                        " -Xms256M -Xmx512M" +
                                        " com.epam.bigdata2016.minskq3.task2.WordProcessor" +
                                        " " + inputPath +
                                        " " + String.valueOf(numbersSegment[0]) +
                                        " " + String.valueOf(numbersSegment[1]) +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));

                Map<String, String> containerEnv = new HashMap<>();
                containerEnv.put("CLASSPATH", "./*");
                ctx.setEnvironment(containerEnv);

                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupAppMasterJar(Constants.HDFS_MY_APP_JAR_PATH, appMasterJar);
                ctx.setLocalResources(Collections.singletonMap(Constants.JAR_NAME, appMasterJar));

                nmClient.startContainer(container, ctx);
            } catch (Exception e) {
                System.err.println("Error with container " + container.getId());
                System.err.println(e.getMessage());
            }
        }
    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(getConfiguration()).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
