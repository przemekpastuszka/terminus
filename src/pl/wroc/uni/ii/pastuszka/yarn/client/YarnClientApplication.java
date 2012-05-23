package pl.wroc.uni.ii.pastuszka.yarn.client;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_ADDRESS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_ADDRESS;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

import pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon;

public class YarnClientApplication {
  private static final Log LOG = LogFactory.getLog(YarnClientApplication.class);

  private ClientRMProtocol applicationsManager;
  private ApplicationSubmissionContext appContext;

  public YarnClientApplication(String name) {
    try {
      applicationsManager = (ClientRMProtocol) YarnCommon.get().connectTo(RM_ADDRESS, DEFAULT_RM_ADDRESS, ClientRMProtocol.class);
      createApplicationSubmissionContext(name);
    } catch (YarnRemoteException e) {
      throw new RuntimeException(e);
    }
  }

  protected void createApplicationSubmissionContext(String name) throws YarnRemoteException {
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
    LOG.info("Got response for new application request. Application id is: " + response.getApplicationId());

    appContext = Records.newRecord(ApplicationSubmissionContext.class);
    appContext.setApplicationId(response.getApplicationId());
    appContext.setApplicationName(name);
  }

  public void submitJob(ContainerContext container) throws YarnRemoteException {
    appContext.setAMContainerSpec(container.getContainerContext());

    SubmitApplicationRequest appRequest =
        Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

    applicationsManager.submitApplication(appRequest);
  }
}
