package wordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Created by qy on 2016/10/25.
 */
public class wordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line, " ");

        while(stringTokenizer.hasMoreElements()) {
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
        }
    }

}
