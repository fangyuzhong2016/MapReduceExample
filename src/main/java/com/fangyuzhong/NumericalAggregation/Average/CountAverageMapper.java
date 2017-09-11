package com.fangyuzhong.NumericalAggregation.Average;

import com.fangyuzhong.NumericalAggregation.CountAverageObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class CountAverageMapper extends Mapper<Object, Text, IntWritable, CountAverageObject>
{

    private IntWritable outHour = new IntWritable();
    private CountAverageObject outCountAverage = new CountAverageObject();
    private final SimpleDateFormat fromat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /**
     * map函数
     *  读取时间和评论长度
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
        if(valueSplits.length>=4)
        {
            String strUserID = valueSplits[1];
            String strUserTime = valueSplits[2];
            String strText = valueSplits[3];
            Date createDate = new Date();
            try
            {
                createDate = fromat.parse(strUserTime);
            } catch (java.text.ParseException ex)
            {

            }
            outHour.set(createDate.getHours());
            outCountAverage.setCount(1);
            outCountAverage.setTextLength(strText.length());
            context.write(outHour,outCountAverage);
        }
    }
}
