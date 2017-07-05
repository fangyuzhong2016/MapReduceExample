package com.fangyuzhong.FilterDesign.TopFilterN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-7-4.
 */
public class DistinctUserReducer extends Reducer<User, NullWritable, User,NullWritable >
{
    /**
     * reduce函数
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(User key, Iterable<NullWritable> values, Context context)
            throws InterruptedException, IOException
    {
       context.write(key,NullWritable.get());
    }
}
