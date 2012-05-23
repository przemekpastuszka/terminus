package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static org.apache.hadoop.yarn.api.ApplicationConstants.AM_CONTAINER_ID_ENV;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_SCHEDULER_ADDRESS;

import java.util.Map;

import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon;

public class ApplicationManager {

  private AMRMProtocol resourceManager;

  public ApplicationManager() {
    resourceManager = (AMRMProtocol) YarnCommon.get().connectTo(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_ADDRESS,
        AMRMProtocol.class);
  }

  public static void main(String[] args) {
    ApplicationManager appManager = new ApplicationManager();
    try {
      appManager.registerApplicationMaster();
    } catch (YarnRemoteException e) {
      throw new RuntimeException(e);
    }
  }

  protected void registerApplicationMaster() throws YarnRemoteException {
    Map<String, String> envs = System.getenv();
    String containerIdString =
        envs.get(AM_CONTAINER_ID_ENV);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

    RegisterApplicationMasterRequest appMasterRequest =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost("localhost");
    appMasterRequest.setRpcPort(8985);
    appMasterRequest.setTrackingUrl("http://localhost:8985");

    RegisterApplicationMasterResponse response =
        resourceManager.registerApplicationMaster(appMasterRequest);
  }
}
