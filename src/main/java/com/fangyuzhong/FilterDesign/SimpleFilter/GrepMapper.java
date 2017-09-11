package com.fangyuzhong.FilterDesign.SimpleFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;

/**
 * 简单的过滤器模式 ，只有map  没有reduce</p>
 * 使用Java内建api处理正则表达式。如果文本行匹配样式，就输出这一行。否则忽略这一行。
 * 我们使用setup方法获取job配置的正则</p>
 * Created by fangyuzhong on 17-6-29.
 */
public class GrepMapper extends Mapper<Object, Text, NullWritable, Text>
{

    private String mapRegex=null;

    public void setup(Context context) throws IOException,InterruptedException
    {
        mapRegex = context.getConfiguration().get("mapregex");
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
        if(value.toString().matches(mapRegex))
        {
            context.write(NullWritable.get(),value);
        }
    }
}
