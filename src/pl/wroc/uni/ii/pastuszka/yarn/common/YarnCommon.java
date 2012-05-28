package pl.wroc.uni.ii.pastuszka.yarn.common;

import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnSettings.CONFIGURATION_DIR;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

public class YarnCommon {
  Configuration conf = new Configuration();
  YarnRPC rpc;

  private static YarnCommon instance = null;

  private YarnCommon() {
    conf.addResource(new Path(CONFIGURATION_DIR + "/core-site.xml"));
    conf.addResource(new Path(CONFIGURATION_DIR + "/hdfs-site.xml"));
    rpc = YarnRPC.create(conf);
  }

  public static YarnCommon get() {
    if (instance == null) {
      instance = new YarnCommon();
    }

    return instance;
  }

  public static Configuration getConfiguration() {
    return get().conf;
  }

  public Object connectToUsingConf(String addressConf, String defaultAddress, Class<?> protocol) {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    return connectTo(yarnConf.get(addressConf, defaultAddress), protocol);
  }

  public Object connectTo(String address, Class<?> protocol) {
    InetSocketAddress rmAddress =
        NetUtils.createSocketAddr(address);
    Configuration appsManagerServerConf = new Configuration(conf);

    return rpc.getProxy(protocol, rmAddress, appsManagerServerConf);
  }

  public static Resource getMemoryResource(int memory) {
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    return capability;
  }

}
