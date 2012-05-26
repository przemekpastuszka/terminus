package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static org.apache.hadoop.yarn.api.ApplicationConstants.AM_CONTAINER_ID_ENV;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_SCHEDULER_ADDRESS;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon.getMemoryResource;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnSettings.CONTAINER_MEMORY;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon;
import pl.wroc.uni.ii.pastuszka.yarn.container.AllocatedContainer;

public class ApplicationManager {
  private static final int SLEEP_TIME = 1000;

  private static final Log LOG = LogFactory.getLog(ApplicationManager.class);

  private int responseId = 0;
  private AMRMProtocol resourceManager;
  private ApplicationAttemptId appAttemptID;
  private Map<ContainerId, ContainerStatus> finishedContainersStatuses = new HashMap<ContainerId, ContainerStatus>();

  private boolean applicationAlive = true;
  private List<ResourceRequest> resourceRequest = null;
  private List<AllocatedContainer> containers = new LinkedList<AllocatedContainer>();

  public ApplicationManager() throws YarnRemoteException {
    resourceManager = (AMRMProtocol) YarnCommon.get().connectToUsingConf(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_ADDRESS,
        AMRMProtocol.class);
    register();

    MainThread mainThread = new MainThread();
    mainThread.start();
  }

  private class MainThread extends Thread {
    @Override
    public void run() {
      try {
        while (applicationAlive) {
          if (resourceRequest != null) {
            sendResourceRequest();
          }
          else {
            containers.addAll(allocateResources(EMPTY_LIST));
          }

          Thread.sleep(SLEEP_TIME);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

  }

  private synchronized void sendResourceRequest() throws YarnRemoteException {
    containers.addAll(allocateResources(resourceRequest));
    resourceRequest = null;
  }

  public List<AllocatedContainer> allocateContainers(int numContainers, int timeout) throws YarnRemoteException,
      InterruptedException {
    long startTime = currentTimeMillis();
    createResourceRequest(numContainers);

    while (containers.size() != numContainers) {
      if (currentTimeMillis() - startTime > timeout) {
        LOG.warn(format("Allocated only %s containers, but wanted %s", containers.size(), numContainers));
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }

    return containers;
  }

  protected synchronized void createResourceRequest(int numContainers) {
    containers = new LinkedList<AllocatedContainer>();

    ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);
    rsrcRequest.setHostName("*");
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    rsrcRequest.setPriority(pri);
    rsrcRequest.setCapability(getMemoryResource(CONTAINER_MEMORY));
    rsrcRequest.setNumContainers(numContainers);
    resourceRequest = asList(rsrcRequest);
  }

  protected List<AllocatedContainer> allocateResources(List<ResourceRequest> resourceRequests) throws YarnRemoteException {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(responseId++);
    req.setApplicationAttemptId(appAttemptID);
    req.addAllReleases(EMPTY_LIST);
    req.addAllAsks(resourceRequests);

    AllocateResponse allocateResponse = resourceManager.allocate(req);
    AMResponse amResp = allocateResponse.getAMResponse();
    addCompletedContainers(amResp.getCompletedContainersStatuses());

    List<AllocatedContainer> containers = new LinkedList<AllocatedContainer>();
    for (Container allocatedContainer : amResp.getAllocatedContainers()) {
      containers.add(new AllocatedContainer(allocatedContainer));
    }

    return containers;
  }

  private void addCompletedContainers(List<ContainerStatus> completedContainersStatuses) {
    for (ContainerStatus status : completedContainersStatuses) {
      finishedContainersStatuses.put(status.getContainerId(), status);
    }
  }

  public ContainerStatus waitFor(AllocatedContainer container) throws YarnRemoteException, InterruptedException {
    ContainerId id = container.getId();
    while (finishedContainersStatuses.containsKey(id) == false) {
      Thread.sleep(SLEEP_TIME);
    }
    return finishedContainersStatuses.get(id);
  }

  public void unregister(FinalApplicationStatus status) throws YarnRemoteException {
    applicationAlive = false;

    FinishApplicationMasterRequest req =
        Records.newRecord(FinishApplicationMasterRequest.class);
    req.setAppAttemptId(appAttemptID);
    req.setFinishApplicationStatus(status);
    resourceManager.finishApplicationMaster(req);
    LOG.info("Application unregistered");
  }

  private void register() throws YarnRemoteException {
    Map<String, String> envs = System.getenv();
    String containerIdString =
        envs.get(AM_CONTAINER_ID_ENV);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    appAttemptID = containerId.getApplicationAttemptId();

    RegisterApplicationMasterRequest appMasterRequest =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost("");
    appMasterRequest.setRpcPort(8985);
    appMasterRequest.setTrackingUrl("");

    RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterRequest);
    LOG.info("Application manager registered");
    LOG.info("Minimal resource capability is: " + response.getMinimumResourceCapability().getMemory());
    LOG.info("Maximum resource capability is: " + response.getMaximumResourceCapability().getMemory());
  }
}
