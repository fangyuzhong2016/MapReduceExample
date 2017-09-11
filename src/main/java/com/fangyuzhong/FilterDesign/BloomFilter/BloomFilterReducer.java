package com.fangyuzhong.FilterDesign.BloomFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *将Map输出的key 直接输出到一个文件中，其他什么都不做
 * Created by fangyuzhong on 17-6-30.
 */
public class BloomFilterReducer extends Reducer<Text, NullWritable, Text, NullWritable>
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
    public void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws InterruptedException, IOException
    {
        context.write(key,NullWritable.get());
    }
}
