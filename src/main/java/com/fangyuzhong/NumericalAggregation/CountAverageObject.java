package com.fangyuzhong.NumericalAggregation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class CountAverageObject implements Writable
{
    private double textLength=0;
    private long count=0;

    public void setTextLength(double textLength)
    {
        this.textLength = textLength;
    }

    public double getTextLength()
    {
        return textLength;
    }

    /**
     *设置用户评论的条数
     * @param count
     */
    public void setCount(long count)
    {
        this.count = count;
    }

    /**
     * 获取用户评论的条数
     * @return
     */
    public long getCount()
    {
        return count;
    }



    /**
     * 反序列化对象
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {
        textLength = in.readDouble();
        count = in.readLong();
    }

    /**
     * 序列化对象
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeDouble(textLength);
        out.writeLong(count);
    }
}
