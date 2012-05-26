package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;

import java.util.Set;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public class ApplicationManagerTest {

  public static void main(String[] args) throws YarnRemoteException, InterruptedException {
    ApplicationManager appManager = new ApplicationManager();
    appManager.register();

    Set<AllocatedContainer> containers = appManager.allocateContainers(2);

    appManager.unregister(SUCCEEDED);
  }

}
