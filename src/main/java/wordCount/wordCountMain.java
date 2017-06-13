package wordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qy on 2016/10/25.
 */
public class wordCountMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new wordCountMain(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = new Configuration();
        deleteDir(conf,args[1]);
        /**
         * 作业配置Job Configuration
         */
//        Job job = Job.getInstance();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(wordCountMain.class);
        job.setJobName("WordCount");

        FileInputFormat.addInputPath(job, new Path(args[0]));  // 指定输入路径,是hdfs文件系统中的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 指定结果输出路径, hdfs文件系统中的路径

        job.setOutputKeyClass(Text.class);  // 指定输出map中的key类型
        job.setOutputValueClass(IntWritable.class);  // 指定输出map中的value类型
        job.setOutputFormatClass(TextOutputFormat.class);  // 指定最后输出结果的存储类型

        job.setMapperClass(wordCountMap.class);  // 指定执行map操作的类
        job.setReducerClass(wordCountReduce.class);  // 指定执行reduce操作的类

        int retureValue = job.waitForCompletion(true) ? 0 : 1;  // 提交任务
//        System.out.println("job.isSuccessful " + job.isSuccessful());
        return retureValue;
    }

    private static void deleteDir(Configuration conf, String dirPath) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
          if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly.");
                } else {
                 System.out.println(targetPath + " deletion failed.");
                }
            }

        }


}

