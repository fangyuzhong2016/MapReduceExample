package com.fangyuzhong.NumericalAggregation.MedianStdDevMemory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class MedianStdDevMemoryMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable>
{

    private IntWritable commentLength = new IntWritable();
    private final LongWritable ONE = new LongWritable(1);
    private IntWritable outHour = new IntWritable();
    private final SimpleDateFormat fromat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        if(valueSplits.length>=4)
        {
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
            commentLength.set(strText.length());
            SortedMapWritable outCommentLength = new SortedMapWritable();
            outCommentLength.put(commentLength,ONE);
            context.write(outHour,outCommentLength);
        }
    }
}
