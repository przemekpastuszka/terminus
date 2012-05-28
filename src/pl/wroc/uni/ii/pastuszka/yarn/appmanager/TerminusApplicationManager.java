package pl.wroc.uni.ii.pastuszka.yarn.appmanager;

import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnCommon.getConfiguration;
import static pl.wroc.uni.ii.pastuszka.yarn.common.YarnSettings.JAR_LOCATION;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import pl.wroc.uni.ii.pastuszka.yarn.common.LocalResourceDescription;
import pl.wroc.uni.ii.pastuszka.yarn.container.AllocatedContainer;

public class TerminusApplicationManager {
  private static final Log LOG = LogFactory.getLog(TerminusApplicationManager.class);

  ApplicationManager appManager;
  DistributedFileSystem dfs;
  LocalResourceDescription jar;

  private String workerName;

  private class TerminusTaskThread extends Thread {
    private static final int MAX_ATTEMPTS = 5;
    String task;
    Map<MD5MD5CRC32FileChecksum, List<Path>> checksumsToPaths = new HashMap<MD5MD5CRC32FileChecksum, List<Path>>();

    public TerminusTaskThread(String task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        for (int attemptId = 0; attemptId < MAX_ATTEMPTS; ++attemptId) {
          Path workerOutputPath = new Path(task + attemptId);
          startWorker(workerOutputPath);

          try {
            MD5MD5CRC32FileChecksum checksum = addChecksumToMap(workerOutputPath);
            List<Path> candidateOutputs = checksumsToPaths.get(checksum);
            if (candidateOutputs.size() > 1) {
              LOG.info("Completed task " + task + " in " + (attemptId + 1) + " attempts");
              setAsResult(candidateOutputs.get(0));
              break;
            }
          } catch (Exception e) {
            LOG.warn("There was a problem with reading checksum for " + workerOutputPath.toString() + " file", e);
          }

        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void setAsResult(Path validPath) throws IOException {
      removeAllOutputsButOne(validPath);
      dfs.rename(validPath, new Path(task));
    }

    protected void removeAllOutputsButOne(Path validPath) throws IOException {
      for (Entry<MD5MD5CRC32FileChecksum, List<Path>> entry : checksumsToPaths.entrySet()) {
        for (Path path : entry.getValue()) {
          if (path.equals(validPath) == false) {
            dfs.delete(path, true);
          }
        }
      }
    }

    protected void startWorker(Path nextOutputPath) throws YarnRemoteException, InterruptedException, URISyntaxException {
      AllocatedContainer container = appManager.allocateContainers(1).get(0);
      container.addResource(jar);
      container.addCommand("/opt/java/bin/java -cp spike.jar" +
          " " + workerName +
          " " + task +
          " " + nextOutputPath.toString());
      container.launch();

      appManager.waitFor(container);
    }

    private MD5MD5CRC32FileChecksum addChecksumToMap(Path workerOutput) throws IOException {
      MD5MD5CRC32FileChecksum fileChecksum = dfs.getFileChecksum(workerOutput);
      if (checksumsToPaths.containsKey(fileChecksum) == false) {
        checksumsToPaths.put(fileChecksum, new LinkedList<Path>());
      }
      checksumsToPaths.get(fileChecksum).add(workerOutput);
      return fileChecksum;
    }
  }

  public void run(String[] args) throws InterruptedException, URISyntaxException, IOException {
    try {
      appManager = new ApplicationManager();
      dfs = (DistributedFileSystem) FileSystem.get(getConfiguration());
      jar = LocalResourceDescription.createFrom("spike.jar", new URI(JAR_LOCATION));

      workerName = args[0];
      assertWorkerClassExists(workerName);

      List<TerminusTaskThread> threads = new LinkedList<TerminusTaskThread>();
      for (int i = 1; i < args.length; ++i) {
        TerminusTaskThread taskThread = new TerminusTaskThread(args[i]);
        taskThread.start();
        threads.add(taskThread);
      }

      for (TerminusTaskThread thread : threads) {
        thread.join();
      }

    } finally {
      appManager.unregister(SUCCEEDED);
    }
  }

  protected void assertWorkerClassExists(String workerName) {
    try {
      Class.forName(workerName);
    } catch (ClassNotFoundException e) {
      LOG.fatal("Worker class: " + workerName + " not found");
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws InterruptedException, URISyntaxException, IOException {
    new TerminusApplicationManager().run(args);
  }
}
