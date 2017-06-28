package com.fangyuzhong.NumericalAggregation.MedianStdDevMemory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class MedianStdDevMemoryCombiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>
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
    public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context)
            throws InterruptedException, IOException
    {
        SortedMapWritable outValue = new SortedMapWritable();
        for (SortedMapWritable v:values)
        {
            Set keys = v.keySet();
            if(keys!=null)
            {
                Iterator iterator = keys.iterator();
                while (iterator.hasNext())
                {
                    LongWritable count =(LongWritable)outValue.get(iterator.next());
                    if(count!=null)
                    {
                        count.set(count.get()+((LongWritable)v.get(count)).get());
                    }
                    else
                    {
                        outValue.put(count,new LongWritable(((LongWritable)v.get(count)).get()));
                    }
                }
            }
        }
        context.write(key,outValue);
    }
}
