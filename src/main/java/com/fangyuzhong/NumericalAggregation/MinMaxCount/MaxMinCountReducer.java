package com.fangyuzhong.NumericalAggregation.MinMaxCount;

import com.fangyuzhong.NumericalAggregation.NumbericalObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class MaxMinCountReducer extends Reducer<Text, NumbericalObject, Text, NumbericalObject>
{

    private NumbericalObject numbericalObject = new NumbericalObject();
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
    public void reduce(Text key, Iterable<NumbericalObject> values, Context context)
            throws InterruptedException, IOException
    {
        numbericalObject.setCount(0);
        numbericalObject.setMin(null);
        numbericalObject.setMin(null);
        int sum=0;
        for(NumbericalObject var:values)
        {
            if(numbericalObject.getMin()==null||
                    var.getMin().compareTo(numbericalObject.getMin())<0)
            {
                numbericalObject.setMin(var.getMin());
            }
            if(numbericalObject.getMax()==null||
                    var.getMax().compareTo(numbericalObject.getMax())>0)
            {
                numbericalObject.setMax(var.getMax());
            }
            sum+=var.getCount();
        }
        numbericalObject.setCount(sum);
        context.write(key,numbericalObject);
    }
}
