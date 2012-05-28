package pl.wroc.uni.ii.pastuszka.yarn.test;

import java.io.IOException;
import java.net.URISyntaxException;

import pl.wroc.uni.ii.pastuszka.yarn.client.TerminusClient;

public class Test {
  public static void main(String[] args) throws URISyntaxException, IOException {
    TerminusClient framework = new TerminusClient();
    framework.setWorker(TestWorker.class);
    framework.addTask("hello");
    framework.run();
  }

}
