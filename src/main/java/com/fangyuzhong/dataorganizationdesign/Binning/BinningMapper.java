package com.fangyuzhong.dataorganizationdesign.Binning;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-7-13.
 */
public class BinningMapper extends Mapper<Object, Text, Text, NullWritable>
{

    private MultipleOutputs<Text,NullWritable> mos = null;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void setup(Context context)
    {
        mos = new MultipleOutputs(context);
    }

    /**
     * map函数
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String[] valueSplits = value.toString().split(",");
        if(valueSplits.length>=7)
        {
            //按照性别输出
            String strMF = valueSplits[2];
            if(strMF!=null&&strMF!="")
            {
                if("M".equals(strMF))
                {
                    mos.write("bins",value,NullWritable.get(),"男-tag");
                }
                else if("F".equals(strMF))
                {
                    mos.write("bins",value,NullWritable.get(),"女-tag");
                }
                else
                {
                    mos.write("bins",value,NullWritable.get(),"其他-tag");
                }
            }
        }
    }

    @Override
    protected  void cleanup(Context context) throws IOException,InterruptedException
    {
        mos.close();
    }
}
