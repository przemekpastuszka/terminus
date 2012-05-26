package pl.wroc.uni.ii.pastuszka.yarn.test;

import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnSettings.CONTAINER_MEMORY;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.appmanager.ApplicationManagerTest;
import pl.wroc.uni.ii.pastuszka.yarn.client.YarnClientApplication;
import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.container.ContainerContext;

public class Test {
  public static void main(String[] args) throws YarnRemoteException {
    YarnClientApplication client = new YarnClientApplication("test_me_yarn");

    ContainerContext context = new ContainerContext();
    context.addResource(LocalResourceDescription.createFromPath("spike.jar", new Path("/tmp/spike.jar")));
    context.addCommand("/opt/java/bin/java -cp spike.jar" +
        " " + ApplicationManagerTest.class.getCanonicalName());

    context.setMemoryConstraint(CONTAINER_MEMORY);

    client.submitJob(context);
  }
}
