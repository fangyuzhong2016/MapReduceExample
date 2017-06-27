package com.fangyuzhong.InvertedIndex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *定义Map输出后中间结果数据合并Reduce函数<p>
 *     先计数，输出Key和Value类型  AddressKeyWord，Text<p>
 * Created by fangyuzhong on 17-6-26.
 */
public  class InvertedIndexCombiner extends Reducer<AddressKeyWord,Text,AddressKeyWord,Text>
{
    public void reduce(AddressKeyWord key, Iterable<Text> values, Context context)
            throws InterruptedException,IOException
    {
        int sum = 0;
        for (Text value : values)
        {
            if(value==null||value.toString()=="") continue;
            try
            {
                sum += Integer.parseInt(value.toString());
            }
           catch (Exception ex)
           {

           }
        }
        context.write(key, new Text(key.getDescription()+ ":" + sum));
    }
}
