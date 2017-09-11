package com.fangyuzhong.dataorganizationdesign.PartitionedDesign;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by fangyuzhong on 17-7-12.
 */
public class PartitionedUserPartitioner extends
        Partitioner<Text, Text>
{

    @Override
    public int getPartition(Text key,Text value,int numPartitions)
    {

        final String MF = key.toString();//获取到性别
        if("M".equals(MF))
        {
            return 0%numPartitions;
        }
        else if("F".equals(MF))
        {
            return 1%numPartitions;
        }
        else
        {
            return 2%numPartitions;
        }
    }
}

