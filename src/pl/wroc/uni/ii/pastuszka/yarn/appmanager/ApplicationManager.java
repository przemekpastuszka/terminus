package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static org.apache.hadoop.yarn.api.ApplicationConstants.AM_CONTAINER_ID_ENV;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_SCHEDULER_ADDRESS;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon;

public class ApplicationManager {
  private static final Log LOG = LogFactory.getLog(ApplicationManager.class);
  private AMRMProtocol resourceManager;
  private ApplicationAttemptId appAttemptID;

  public ApplicationManager() {
    resourceManager = (AMRMProtocol) YarnCommon.get().connectTo(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_ADDRESS,
        AMRMProtocol.class);
  }

  public static void main(String[] args) throws IOException {
    try {
      ApplicationManager appManager = new ApplicationManager();
      appManager.registerApplicationMaster();
      appManager.unregister(SUCCEEDED);
    } catch (YarnRemoteException e) {
      throw new RuntimeException(e);
    }
  }

  private void unregister(FinalApplicationStatus status) throws YarnRemoteException {
    FinishApplicationMasterRequest req =
        Records.newRecord(FinishApplicationMasterRequest.class);
    req.setAppAttemptId(appAttemptID);
    req.setFinishApplicationStatus(status);
    resourceManager.finishApplicationMaster(req);
    LOG.info("Application unregistered");
  }

  protected void registerApplicationMaster() throws YarnRemoteException {
    Map<String, String> envs = System.getenv();
    String containerIdString =
        envs.get(AM_CONTAINER_ID_ENV);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    appAttemptID = containerId.getApplicationAttemptId();

    RegisterApplicationMasterRequest appMasterRequest =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost("localhost");
    appMasterRequest.setRpcPort(8985);
    appMasterRequest.setTrackingUrl("http://localhost:8985");

    resourceManager.registerApplicationMaster(appMasterRequest);
    LOG.info("Application manager registered");
  }
}
