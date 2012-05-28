package pl.wroc.uni.ii.pastuszka.yarn.client;

import static org.apache.commons.lang.StringUtils.join;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnSettings.CONTAINER_MEMORY;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnSettings.JAR_LOCATION;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.appmanager.TerminusApplicationManager;
import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.container.ContainerContext;

public class TerminusClient {

  Class<?> worker;
  List<String> tasks = new LinkedList<String>();

  public void setWorker(Class<?> worker) {
    this.worker = worker;
  }

  public void addTask(String task) {
    tasks.add(task);
  }

  public void run() {
    try {
      YarnClientApplication client = new YarnClientApplication("test_me_yarn");

      ContainerContext context = new ContainerContext();
      context.addResource(LocalResourceDescription.createFrom("spike.jar", new URI(JAR_LOCATION)));
      context.addCommand("/opt/java/bin/java -cp spike.jar" +
          " " + TerminusApplicationManager.class.getCanonicalName() +
          " " + worker.getCanonicalName() +
          " " + join(tasks, " "));

      context.setMemoryConstraint(CONTAINER_MEMORY);

      client.submitJob(context);
    } catch (YarnRemoteException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
