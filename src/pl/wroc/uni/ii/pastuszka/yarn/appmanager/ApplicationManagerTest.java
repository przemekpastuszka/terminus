package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static java.lang.String.format;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.container.AllocatedContainer;
import pl.wroc.uni.ii.pastuszka.yarn.container.ContainerTest;

public class ApplicationManagerTest {
  private static final Log LOG = LogFactory.getLog(ApplicationManagerTest.class);
  private static final int TIMEOUT = 7000;

  public static void main(String[] args) throws YarnRemoteException, InterruptedException {
    ApplicationManager appManager = new ApplicationManager();
    try {
      AllocatedContainer container = appManager.allocateContainers(1, TIMEOUT).get(0);
      container.addResource(LocalResourceDescription.createFromPath("spike.jar", new Path("/tmp/spike.jar")));
      container.addCommand("/opt/java/bin/java -cp spike.jar" +
          " " + ContainerTest.class.getCanonicalName());
      container.launch();

      ContainerStatus status = appManager.waitFor(container);
      LOG.info(format("Container finished with %d exit code and %s status", status.getExitStatus(), status.getDiagnostics()));

    } finally {
      appManager.unregister(SUCCEEDED);
    }
  }
}
