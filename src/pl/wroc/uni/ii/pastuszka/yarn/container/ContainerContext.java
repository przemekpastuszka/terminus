package pl.wroc.uni.ii.pastuszka.yarn.container;

import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon.getMemoryResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;


public class ContainerContext {
  private ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
  private Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
  private List<String> commands = new ArrayList<String>();

  public static ContainerContext fromContainer(Container container) {
    ContainerContext context = new ContainerContext();
    context.amContainer.setContainerId(container.getId());
    context.amContainer.setResource(container.getResource());
    return context;
  }

  public void addResource(LocalResourceDescription resource) {
    localResources.put(resource.getAlias(), resource.getLocalRes());
  }

  public void addCommand(String command) {
    commands.add(command +
        " 1>" + LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + LOG_DIR_EXPANSION_VAR + "/stderr");
  }

  public void setMemoryConstraint(int memory) {
    amContainer.setResource(getMemoryResource(memory));
  }

  public ContainerLaunchContext getContainerContext() {
    try {
      amContainer.setLocalResources(localResources);
      amContainer.setCommands(commands);
      amContainer.setUser(getCurrentUser().getShortUserName());

      return amContainer;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
