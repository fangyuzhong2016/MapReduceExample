package com.fangyuzhong.NumericalAggregation.MedianStdDev;

import com.fangyuzhong.NumericalAggregation.MedianStdDevObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class MedianStandardDeviationReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevObject>
{

    private MedianStdDevObject result = new MedianStdDevObject();
    private ArrayList<Double> commentLengths = new ArrayList<Double>();

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
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws InterruptedException, IOException
    {

        double sum=0;
        double count=0;
        commentLengths.clear();
        result.setStdDev(0);
        for(IntWritable val:values)
        {
            commentLengths.add((double) val.get());
            sum+=val.get();
            ++count;
        }
        //评论长度排序
        Collections.sort(commentLengths);
        //计算中位数
        if(count%2==0)
        {
            double dMedian = commentLengths.get((int)count/2-1)
                    +commentLengths.get((int)count/2);
            result.setMedian(dMedian/2.0);
        }
        else
        {
            result.setMedian(commentLengths.get((int)count/2));
        }
        //计算标准差
        double mean = sum /count;
        double sumOfSquares =0.0;
        for(Double dVar : commentLengths)
        {
            sumOfSquares+=(dVar-mean)*(dVar-mean);
        }
        result.setStdDev(Math.sqrt(sumOfSquares/(count-1)));
        context.write(key,result);
    }
}
