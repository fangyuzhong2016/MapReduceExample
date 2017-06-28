package com.fangyuzhong.NumericalAggregation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 用户评论数据输出的结果对象包括 评论最早时间、最后时间和评论总条数
 * Created by fangyuzhong on 17-6-28.
 */
public class NumbericalObject implements Writable
{
    private Date max = new Date();
    private Date min = new Date();
    private long count=0;
    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     *设置最后一次评论时间
     * @param max
     */
    public void setMax(Date max)
    {
        this.max=max;
    }

    /**
     *获取最后一次评论时间
     * @return
     */
    public Date getMax()
    {
        return max;
    }

    /**
     *设置第一次评论时间
     * @param min
     */
    public void setMin(Date min)
    {
        this.min=min;
    }

    /**
     *获取第一次评论时间
     * @return
     */
    public Date getMin()
    {
        return min;
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
        //按照序列号write的顺序读取
        min = new Date(in.readLong());
        max = new Date(in.readLong());
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
        out.writeLong(min.getTime());
        out.writeLong(max.getTime());
        out.writeLong(count);
    }

    public String toString()
    {
        return simpleDateFormat.format(min)+"\t"+simpleDateFormat.format(max)+"\t"+count;
    }
}
