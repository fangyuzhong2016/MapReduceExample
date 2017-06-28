package com.fangyuzhong.NumericalAggregation.NumericalMinMaxCount;

import com.fangyuzhong.NumericalAggregation.NumbericalObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 数值聚合计算的最大值最小值数量的Map类
 * Created by fangyuzhong on 17-6-28.
 */
public class MaxMinCountMapper extends Mapper<Object, Text, Text, NumbericalObject>
{

    private Text userID=new Text();

    private NumbericalObject numbericalObject = new NumbericalObject();
    private final SimpleDateFormat fromat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * map函数，map的输入Value的值样式：<p>
     *     商品ID、用户ID、评论时间、评论内容
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
            String struUserID = valueSplits[1];
            String strUserTime = valueSplits[2];
            Date createDate = new Date();
            try
            {
                createDate = fromat.parse(strUserTime);
            } catch (java.text.ParseException ex)
            {

            }
            numbericalObject.setMax(createDate);
            numbericalObject.setMin(createDate);
            numbericalObject.setCount(1);

            userID.set(struUserID);
            context.write(userID,numbericalObject);
        }
    }
}
