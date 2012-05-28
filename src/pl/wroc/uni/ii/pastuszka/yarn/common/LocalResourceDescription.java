package pl.wroc.uni.ii.pastuszka.yarn.common;

import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon.getConfiguration;

import java.io.IOException;
import java.net.URI;

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

  public static LocalResourceDescription createFrom(String alias, URI path) {
    try {
      FileSystem fs = FileSystem.get(path, getConfiguration());
      return createFrom(alias, fs.getFileStatus(new Path(path)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static LocalResourceDescription createFrom(String alias, FileStatus jarStatus) {
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

    amJarRsrc.setType(LocalResourceType.FILE);
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarStatus.getPath()));
    amJarRsrc.setTimestamp(jarStatus.getModificationTime());
    amJarRsrc.setSize(jarStatus.getLen());

    return new LocalResourceDescription(amJarRsrc, alias);
  }

  public LocalResource getLocalRes() {
    return amJarRsrc;
  }

  public String getAlias() {
    return alias;
  }
}
