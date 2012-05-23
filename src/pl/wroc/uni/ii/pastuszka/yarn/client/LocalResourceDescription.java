package pl.wroc.uni.ii.pastuszka.yarn.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class LocalResourceDescription {

  LocalResource amJarRsrc;
  String alias;

  private LocalResourceDescription(LocalResource amJarRsrc, String alias) {
    this.amJarRsrc = amJarRsrc;
    this.alias = alias;
  }

  public static LocalResourceDescription createFromPath(String alias, Path path) {
    try {
      LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus jarStatus = fs.getFileStatus(path);

      amJarRsrc.setType(LocalResourceType.FILE);
      amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(path));
      amJarRsrc.setTimestamp(jarStatus.getModificationTime());
      amJarRsrc.setSize(jarStatus.getLen());

      return new LocalResourceDescription(amJarRsrc, alias);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public LocalResource getLocalRes() {
    return amJarRsrc;
  }

  public String getAlias() {
    return alias;
  }
}
