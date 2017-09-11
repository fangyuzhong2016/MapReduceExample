package com.fangyuzhong.NumericalAggregation.Average;

import com.fangyuzhong.NumericalAggregation.CountAverageObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class CountAverageReducer extends Reducer<IntWritable, CountAverageObject, IntWritable, CountAverageObject>
{
    private  CountAverageObject result = new CountAverageObject();
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
    public void reduce(IntWritable key, Iterable<CountAverageObject> values, Context context)
            throws InterruptedException, IOException
    {

        double sum=0;
        int count=0;
        for(CountAverageObject val :values)
        {
            sum+=val.getCount()*val.getTextLength();
            count +=val.getCount();
        }
        result.setCount(count);
        result.setTextLength(sum/(count*1.0));

        context.write(key,result);
    }
}
