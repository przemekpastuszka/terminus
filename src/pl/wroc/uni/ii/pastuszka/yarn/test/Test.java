package pl.wroc.uni.ii.pastuszka.yarn.test;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.client.YarnClientApplication;

public class Test {

  public static void main(String[] args) throws YarnRemoteException {
    YarnClientApplication client = new YarnClientApplication("test_me_yarn");
  }
}
