package com.fangyuzhong.NumericalAggregation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by fangyuzhong on 17-6-28.
 */
public class MedianStdDevObject implements Writable
{

    private double stdDev=0.0;
    private double median=0.0;

    public void setStdDev(double stdDev)
    {
        this.stdDev=stdDev;
    }

    public double getStdDev()
    {
        return stdDev;
    }

    public void setMedian(double median)
    {
        this.median=median;
    }
    public double getMedian()
    {
        return median;
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
        median = in.readDouble();
        stdDev=in.readDouble();
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
        out.writeDouble(median);
        out.writeDouble(stdDev);
    }

    @Override
    public  String toString()
    {
        return "中位数："+median+"  标准差："+stdDev;
    }
}
