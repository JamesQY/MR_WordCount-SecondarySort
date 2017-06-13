package secondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SortReducer extends Reducer<CombinationKey, IntWritable, Text, Text>
{
    private  StringBuffer sb = new StringBuffer();
    private  Text sore = new Text();
    /**
     * 这里要注意一下reduce的调用时机和次数:reduce每处理一个分组的时候会调用一
     * 次reduce函数。也许有人会疑问，分组是什么？看个例子就明白了：
     * eg:
     * {{sort1,{1,2}},{sort2,{3,54,77}},{sort6,{20,22,221}}}
     * 这个数据结果是分组过后的数据结构，那么一个分组分别为{sort1,{1,2}}、
     * {sort2,{3,54,77}}、{sort6,{20,22,221}}
     */
    @Override
    protected void reduce(CombinationKey key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        sb.delete(0, sb.length());//先清除上一个组的数据
        Iterator<IntWritable> it = value.iterator();

        while(it.hasNext()){
            sb.append(it.next()+",");
        }
        //去除最后一个逗号
        if(sb.length()>0){
            sb.deleteCharAt(sb.length()-1);
        }
        sore.set(sb.toString());
        context.write(key.getFirstKey(),sore);
        SecondSortMR.logger.info("---------enter reduce function flag---------");
        SecondSortMR.logger.info("reduce Input data:{["+key.getFirstKey()+","+
                key.getSecondKey()+"],["+sore+"]}");
        SecondSortMR.logger.info("---------out reduce function flag---------");
    }
}
