package pl.wroc.uni.ii.pastuszka.yarn.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon;

public class AllocatedContainer {
  private static final Log LOG = LogFactory.getLog(AllocatedContainer.class);
  private Container allocatedContainer;
  private ContainerContext context;
  private ContainerManager containerManager;

  public AllocatedContainer(Container container) {
    this.allocatedContainer = container;

    LOG.info("Allocated new container."
        + "\nid: " + allocatedContainer.getId()
        + "\nnode: " + allocatedContainer.getNodeId().getHost()
        + ":" + allocatedContainer.getNodeId().getPort()
        + "\nURI: " + allocatedContainer.getNodeHttpAddress()
        + "\nstate: " + allocatedContainer.getState()
        + "\nmemory: "
        + allocatedContainer.getResource().getMemory());

    context = ContainerContext.fromContainer(container);

    String containerManagerAddress = container.getNodeId().getHost() + ":"
        + container.getNodeId().getPort();
    containerManager = (ContainerManager) YarnCommon.get().connectTo(containerManagerAddress, ContainerManager.class);
  }

  public ContainerId getId() {
    return allocatedContainer.getId();
  }

  public void addResource(LocalResourceDescription resource) {
    context.addResource(resource);
  }

  public void addCommand(String command) {
    context.addCommand(command);
  }

  public void launch() throws YarnRemoteException {
    StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
    startReq.setContainerLaunchContext(context.getContainerContext());
    containerManager.startContainer(startReq);
  }

  @Override
  public int hashCode() {
    return allocatedContainer.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AllocatedContainer) {
      AllocatedContainer other = (AllocatedContainer) obj;
      return allocatedContainer.equals(other.allocatedContainer);
    }
    return false;
  }
}
