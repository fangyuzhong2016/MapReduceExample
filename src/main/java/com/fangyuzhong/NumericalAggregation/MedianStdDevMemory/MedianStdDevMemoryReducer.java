package com.fangyuzhong.NumericalAggregation.MedianStdDevMemory;

import com.fangyuzhong.NumericalAggregation.MedianStdDevObject;
import com.sun.xml.internal.bind.v2.util.QNameMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class MedianStdDevMemoryReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevObject>
{
    private MedianStdDevObject result = new MedianStdDevObject();
    private TreeMap<Integer,Long> commentLengthCounts = new TreeMap<Integer,Long>();
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
        double sum=0.0;
        long totalComments=0;
        commentLengthCounts.clear();
        result.setMedian(0);
        result.setStdDev(0);
        for(SortedMapWritable v:values)
        {
            Set keys = v.keySet();
            if(keys!=null)
            {
                Iterator iterator = keys.iterator();
                while (iterator.hasNext())
                {
                    int length = ((IntWritable)iterator.next()).get();
                    long count = ((LongWritable)v.get(length)).get();
                    totalComments+=count;
                    sum+=length*count;
                    Long storedCount = commentLengthCounts.get(length);
                    if(storedCount==null)
                    {
                        commentLengthCounts.put(length,count);
                    }
                    else
                    {
                        commentLengthCounts.put(length,storedCount+count);
                    }
                }
            }
        }
        long medianIndex = totalComments/2L;
        long previousComments =0;
        long comments=0;
        int prevKey=0;
        for(Entry<Integer,Long> entry:commentLengthCounts.entrySet())
        {
            comments = previousComments+entry.getValue();
            if(previousComments<=medianIndex&&medianIndex<comments)
            {
                if(totalComments%2==0&&previousComments==medianIndex)
                {
                    result.setMedian((entry.getKey()+prevKey)/2.0);
                }
                else
                {
                    result.setMedian(entry.getKey());
                }
                break;
            }
            previousComments = comments;
            prevKey = entry.getKey();
        }
        double mean = sum/totalComments;
        double sumOfSquares =0.0;
        for(Entry<Integer,Long> entry:commentLengthCounts.entrySet())
        {
            sumOfSquares+=(entry.getKey()-mean)*(entry.getKey()-mean)*entry.getValue();
        }
        result.setStdDev(Math.sqrt(sumOfSquares/(totalComments-1)));
        context.write(key,result);
    }
}
