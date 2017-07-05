package com.fangyuzhong.FilterDesign.TopFilterN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by fangyuzhong on 17-7-4.
 */
public class TopFilterNReducer extends Reducer<NullWritable, User, NullWritable, User>
{
   private SortedMap<Integer,User> reduceTopN = new TreeMap<>();
    private int N =10;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        N  = context.getConfiguration().getInt("top.n",10);
    }

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
    public void reduce(NullWritable key, Iterable<User> values, Context context)
            throws InterruptedException, IOException
    {
        for(User var:values)
        {
            reduceTopN.put(var.getAge(),var);
            if(reduceTopN.size()>N)
            {
             reduceTopN.remove(reduceTopN.firstKey());
            }
        }
        for(User var:reduceTopN.values())
        {
            context.write(NullWritable.get(),var);
        }
    }
}
