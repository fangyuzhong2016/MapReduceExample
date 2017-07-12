package com.fangyuzhong.DataOrganizationDesign.PartitionedDesign;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by fangyuzhong on 17-7-12.
 */
public class PartitionedUsersMapper extends Mapper<Object, Text, Text, Text>
{

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
                context.write(new Text(strMF),value);
            }

        }
    }
}
