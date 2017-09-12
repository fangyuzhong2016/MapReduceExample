package com.fangyuzhong.joindesign.compositejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;

import java.io.IOException;

/**
 * 复合连接的Mapper类
 * Created by fangyuzhong on 17-9-12.
 */
public class CompositeMapper extends MapReduceBase
        implements Mapper<Text, TupleWritable, Text, Text>
{

    @Override
    public void map(Text key, TupleWritable value,
                    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException
    {
        //从输入的集合中取出前两个输出
        output.collect((Text) value.get(0), (Text) value.get(1));
    }
}
