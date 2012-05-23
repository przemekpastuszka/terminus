package pl.wroc.uni.ii.pastuszka.yarn.client;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

public class YarnClientApplication {
  private static final Log LOG = LogFactory.getLog(YarnClientApplication.class);

  String name;
  YarnRPC rpc;
  Configuration conf = new Configuration();
  private ClientRMProtocol applicationsManager;
  private ApplicationSubmissionContext appContext;

  public YarnClientApplication(String name) {
    this.name = name;

    try {
      rpc = YarnRPC.create(conf);
      connectToApplicationsManager();
      createApplicationSubmissionContext();
    } catch (YarnRemoteException e) {
      throw new RuntimeException(e);
    }
  }

  protected void createApplicationSubmissionContext() throws YarnRemoteException {
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
    LOG.info("Got response for new application request. Application id is: " + response.getApplicationId());

    appContext = Records.newRecord(ApplicationSubmissionContext.class);
    appContext.setApplicationId(response.getApplicationId());
    appContext.setApplicationName(name);
  }

  protected void connectToApplicationsManager() {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress =
        NetUtils.createSocketAddr(yarnConf.get(
            YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS));
    Configuration appsManagerServerConf = new Configuration(conf);

    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, appsManagerServerConf));
    LOG.info("Connected to application manager");
  }

  public void submitJob(ContainerContext container) throws YarnRemoteException {
    appContext.setAMContainerSpec(container.getContainerContext());

    SubmitApplicationRequest appRequest =
        Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

    applicationsManager.submitApplication(appRequest);
  }
}
