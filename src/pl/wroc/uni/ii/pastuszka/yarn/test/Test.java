package pl.wroc.uni.ii.pastuszka.yarn.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.client.ContainerContext;
import pl.wroc.uni.ii.pastuszka.yarn.client.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.client.YarnClientApplication;

public class Test {

  public static void main(String[] args) throws YarnRemoteException {
    YarnClientApplication client = new YarnClientApplication("test_me_yarn");

    ContainerContext context = new ContainerContext();
    context.addCommand("/usr/bin/echo test");
    context.addResource(LocalResourceDescription.createFromPath("hej", new Path("/home/rtshadow/tmp")));
    context.setMemoryConstraint(10000);

    client.submitJob(context);
  }
}
