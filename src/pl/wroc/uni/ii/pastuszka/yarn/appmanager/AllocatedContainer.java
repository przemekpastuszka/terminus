package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;

public class AllocatedContainer {
  private static final Log LOG = LogFactory.getLog(AllocatedContainer.class);
  private Container allocatedContainer;

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
