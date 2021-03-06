package secondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortMapper extends Mapper<Text, Text, CombinationKey, IntWritable>
{
    //---------------------------------------------------------
    /**
     * 这里特殊要说明一下，为什么要将这些变量写在map函数外边。
     * 对于分布式的程序，我们一定要注意到内存的使用情况，对于mapreduce框架，
     * 每一行的原始记录的处理都要调用一次map函数，假设，此个map要处理1亿条输
     * 入记录，如果将这些变量都定义在map函数里边则会导致这4个变量的对象句柄编
     * 程非常多（极端情况下将产生4*1亿个句柄，当然java也是有自动的gc机制的，
     * 一定不会达到这么多，但是会浪费很多时间去GC），导致栈内存被浪费掉。我们将其写在map函数外边，
     * 顶多就只有4个对象句柄。
     */
    private   CombinationKey combinationKey = new CombinationKey();
    private Text sortName = new Text();
    private IntWritable score = new IntWritable();
    //---------------------------------------------------------
//    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        SecondSortMR.logger.info("---------enter map function flag---------");
        SecondSortMR.logger.info(key.toString()+"---"+value.toString());
        //过滤非法记录
        if(key == null || value == null || key.toString().equals("")
                || value.toString().equals("")){
            return;
        }
        sortName.set(key.toString());
        score.set(Integer.parseInt(value.toString()));
        combinationKey.setFirstKey(sortName);
        combinationKey.setSecondKey(score);
        //map输出
        context.write(combinationKey, score);
        SecondSortMR.logger.info("---------out map function flag---------");
    }
}

