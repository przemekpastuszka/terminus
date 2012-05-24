package pl.wroc.uni.ii.pastuszka.yarn.test;

import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.appmanager.ApplicationManager;
import pl.wroc.uni.ii.pastuszka.yarn.client.ContainerContext;
import pl.wroc.uni.ii.pastuszka.yarn.client.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.client.YarnClientApplication;

public class Test {

  public static void main(String[] args) throws YarnRemoteException {
    YarnClientApplication client = new YarnClientApplication("test_me_yarn");

    ContainerContext context = new ContainerContext();
    context.addResource(LocalResourceDescription.createFromPath("spike.jar", new Path("/tmp/spike.jar")));
    context.addCommand("/opt/java/bin/java" +
        " " + ApplicationManager.class.getCanonicalName() +
        " 1>" + LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + LOG_DIR_EXPANSION_VAR + "/stderr");

    context.setMemoryConstraint(3000);

    client.submitJob(context);
  }
}
