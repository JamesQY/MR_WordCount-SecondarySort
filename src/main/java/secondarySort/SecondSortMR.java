package secondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by qy on 2016/10/27.
 */
public class SecondSortMR extends Configured implements Tool
{
    public static final Logger logger = LoggerFactory.getLogger(SecondSortMR.class);
//    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf(); //获得配置文件对象
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        deleteDir(conf,args[1]);

        Job job=new Job(conf,"SoreSort");
        job.setJarByClass(SecondSortMR.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); //设置map输入文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //设置reduce输出文件路径

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setPartitionerClass(DefinedPartition.class); //设置自定义分区策略

        job.setGroupingComparatorClass(DefinedGroupSort.class); //设置自定义分组策略
        job.setSortComparatorClass(DefinedComparator.class); //设置自定义二次排序策略

        job.setInputFormatClass(KeyValueTextInputFormat.class); //设置文件输入格式
        job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格式



        //设置map的输出key和value类型
        job.setMapOutputKeyClass(CombinationKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return job.isSuccessful()?0:1;
    }

    public static void main(String[] args) {
        try {
            int returnCode =  ToolRunner.run(new SecondSortMR(),args);
            System.exit(returnCode);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

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