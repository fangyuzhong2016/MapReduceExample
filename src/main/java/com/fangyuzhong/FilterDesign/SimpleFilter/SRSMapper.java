package com.fangyuzhong.FilterDesign.SimpleFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.Random;

/**
 * 从setup方法里获取过滤器的百分率配置值，在map方法里会用到。</p>
 * Map中，检查随机数的生成。随机数在0到1之间，所以根据与临界值的比较，可以决定是否保留记录</p>
 * Created by fangyuzhong on 17-6-29.
 */
public class SRSMapper extends Mapper<Object, Text, NullWritable, Text>
{

    private Random randoms = new Random();
    private Double precentage;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        //通过设置配置文件 设置百分率 如：conf.set("filter_percentage", .5);
        String strPercentage = context.getConfiguration().get("filter_percentage");
        precentage = Double.parseDouble(strPercentage)/100.0;
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
        if(randoms.nextDouble()<precentage)
        {
            context.write(NullWritable.get(),value);
        }
    }
}
