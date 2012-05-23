package pl.wroc.uni.ii.pastuszka.yarn.common;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

public class YarnCommon {
  Configuration conf = new Configuration();
  YarnRPC rpc;

  private static YarnCommon instance = null;

  private YarnCommon() {
    rpc = YarnRPC.create(conf);
  }

  public static YarnCommon get() {
    if (instance == null) {
      instance = new YarnCommon();
    }

    return instance;
  }

  public Object connectTo(String addressConf, String defaultAddress, Class<?> protocol) {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress =
        NetUtils.createSocketAddr(yarnConf.get(addressConf, defaultAddress));
    Configuration appsManagerServerConf = new Configuration(conf);

    return rpc.getProxy(protocol, rmAddress, appsManagerServerConf);
  }

}
