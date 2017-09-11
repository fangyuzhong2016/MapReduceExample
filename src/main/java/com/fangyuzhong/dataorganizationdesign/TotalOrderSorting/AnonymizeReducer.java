package com.fangyuzhong.dataorganizationdesign.TotalOrderSorting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-7-13.
 */
public class AnonymizeReducer extends Reducer<IntWritable, Text, Text, NullWritable>
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
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws InterruptedException, IOException
    {

        for (Text t : values)
        {
            context.write(t, NullWritable.get());
        }
    }
}
