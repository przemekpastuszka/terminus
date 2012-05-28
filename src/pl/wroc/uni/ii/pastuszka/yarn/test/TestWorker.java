package pl.wroc.uni.ii.pastuszka.yarn.test;

import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon.getConfiguration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestWorker {

  public static void main(String[] args) throws IOException, URISyntaxException {
    String task = args[0];
    String outputFile = args[1];

    FileSystem fs = FileSystem.get(getConfiguration());
    FSDataOutputStream stream = fs.create(new Path(outputFile), true);

    stream.writeChars(Boolean.toString(getBoolean(0.3f)));

    stream.close();
  }

  public static boolean getBoolean(float probability) {
    Random rand = new Random();
    return rand.nextFloat() < probability;
  }
}
