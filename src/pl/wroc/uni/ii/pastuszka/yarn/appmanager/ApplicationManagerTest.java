package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;

import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.container.AllocatedContainer;
import pl.wroc.uni.ii.pastuszka.yarn.container.ContainerTest;

public class ApplicationManagerTest {

  public static void main(String[] args) throws YarnRemoteException, InterruptedException {
    ApplicationManager appManager = new ApplicationManager();
    appManager.register();

    AllocatedContainer container = getOneContainer(appManager);
    container.addResource(LocalResourceDescription.createFromPath("spike.jar", new Path("/tmp/spike.jar")));
    container.addCommand("/opt/java/bin/java -cp spike.jar" +
        " " + ContainerTest.class.getCanonicalName());
    container.launch();

    appManager.unregister(SUCCEEDED);
  }

  protected static AllocatedContainer getOneContainer(ApplicationManager appManager) throws YarnRemoteException, InterruptedException {
    AllocatedContainer container = new LinkedList<AllocatedContainer>(appManager.allocateContainers(1)).get(0);
    return container;
  }

}
