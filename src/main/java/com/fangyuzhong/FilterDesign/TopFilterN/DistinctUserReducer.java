package com.fangyuzhong.FilterDesign.TopFilterN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-7-4.
 */
public class DistinctUserReducer extends Reducer<Text, User, User,NullWritable >
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
    public void reduce(Text key, Iterable<User> values, Context context)
            throws InterruptedException, IOException
    {
        User outUser =null;
        for(User var:values)
        {
            if(var.getId().equals(key.toString()))
            {
                outUser=var;
                break;
            }
        }
        if(outUser!=null)
        {
            context.write(outUser, NullWritable.get());
        }
    }
}
