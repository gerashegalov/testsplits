import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.List;

public class SplitBench implements Tool {
  private Configuration conf;

  public static void main(String[] args) {
    int ec = -1;
    try {
      ec = ToolRunner.run(new Configuration(), new SplitBench(), args);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (ec == -1) {
      System.out.println("Usage: " + SplitBench.class + " [ generic ] "
          + " path1...");
      ToolRunner.printGenericCommandUsage(System.out);
    }
    System.exit(ec);
  }

  public int run(String[] args) throws Exception {
    System.out.println("Testing getSplits paths: " + Arrays.toString(args));
    final Job job = Job.getInstance(conf);
    final LzoTextInputFormat inputFormat = new LzoTextInputFormat();
    FileInputFormat.setInputPaths(job, StringUtils.stringToPath(args));
    final long start = System.currentTimeMillis();
    final List<InputSplit> splits = inputFormat.getSplits(job);
    final long duration = System.currentTimeMillis() - start;
    System.out.println("getSplits took (ms): " + duration + " for "
        + splits.size() + " splits");
    return 0;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }
}
