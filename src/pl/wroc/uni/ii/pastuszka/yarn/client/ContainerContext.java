package pl.wroc.uni.ii.pastuszka.yarn.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

public class ContainerContext {
  private ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
  private Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
  private List<String> commands = new ArrayList<String>();

  public ContainerContext() {
    setupClasspath();
  }

  private void setupClasspath() {
    Map<String, String> env = new HashMap<String, String>();
    String classPathEnv = "<CLASSPATH>:./*:";
    env.put("CLASSPATH", classPathEnv);
    amContainer.setEnvironment(env);
  }

  public void addResource(LocalResourceDescription resource) {
    localResources.put(resource.getAlias(), resource.getLocalRes());
  }

  public void addCommand(String command) {
    commands.add(command);
  }

  public void setMemoryConstraint(int memory) {
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    amContainer.setResource(capability);
  }

  public ContainerLaunchContext getContainerContext() {
    amContainer.setLocalResources(localResources);
    amContainer.setCommands(commands);
    return amContainer;
  }
}
