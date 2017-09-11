package com.fangyuzhong.wordcount;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.io.IOException;

/**
 * Created by fangyuzhong on 17-7-3.
 */
public class WordCountTest
{
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private MapReduceDriver mapReduceDriver;

    @Before
    public void setUp()
    {
        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
        reduceDriver =ReduceDriver.newReduceDriver();
        reduceDriver.withReducer(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException
    {
        Text text = new Text("PerName,varchar(255),CHARACTER,PerName,SET,utf8,NULL,DEFAULT,NULL");
        mapDriver.withInput(new LongWritable(),text);
        mapDriver.withOutput(new Text("PerName"),new IntWritable(1));
        mapDriver.withOutput(new Text("varchar(255)"),new IntWritable(1));
        mapDriver.withOutput(new Text("CHARACTER"),new IntWritable(1));
        mapDriver.withOutput(new Text("PerName"),new IntWritable(1));
        mapDriver.withOutput(new Text("SET"),new IntWritable(1));
        mapDriver.withOutput(new Text("utf8"),new IntWritable(1));
        mapDriver.withOutput(new Text("NULL"),new IntWritable(1));
        mapDriver.withOutput(new Text("DEFAULT"),new IntWritable(1));
        mapDriver.withOutput(new Text("NULL"),new IntWritable(1));

        mapDriver.runTest();
        // 输出
        List<Pair> expectedOutputList = mapDriver.getExpectedOutputs();
        for(Pair pair : expectedOutputList)
        {
            System.out.println(pair.getFirst() + " --- " + pair.getSecond());
        }

    }

    @Test
    public void testReduce() throws IOException
    {
        List<IntWritable> intWritableList = Lists.newArrayList();
        intWritableList.add(new IntWritable(1));
        intWritableList.add(new IntWritable(1));
        reduceDriver.withInput(new Text("PerName"),intWritableList);
        reduceDriver.withOutput(new Text("PerName"),new IntWritable(2));
        reduceDriver.runTest();
        // 输出
        List<Pair> expectedOutputList = reduceDriver.getExpectedOutputs();
        for(Pair pair : expectedOutputList)
        {
            System.out.println(pair.getFirst() + " --- " + pair.getSecond());
        }
    }

    @Test
    public void testMapperAndReducer() throws  IOException
    {
        Text text = new Text("PerName,varchar(255),CHARACTER,PerName,SET,utf8,NULL,DEFAULT,NULL");
        mapReduceDriver.withInput(new LongWritable(),text);
        mapReduceDriver.withOutput(new Text("CHARACTER"),new IntWritable(1));
        mapReduceDriver.withOutput(new Text("DEFAULT"),new IntWritable(1));
        mapReduceDriver.withOutput(new Text("NULL"),new IntWritable(2));
        mapReduceDriver.withOutput(new Text("PerName"),new IntWritable(2));
        mapReduceDriver.withOutput(new Text("SET"),new IntWritable(1));
        mapReduceDriver.withOutput(new Text("utf8"),new IntWritable(1));
        mapReduceDriver.withOutput(new Text("varchar(255)"),new IntWritable(1));

        mapReduceDriver.runTest();
        // 输出
        List<Pair> expectedOutputList = mapReduceDriver.getExpectedOutputs();
        for(Pair pair : expectedOutputList)
        {
            System.out.println(pair.getFirst() + " ---- " + pair.getSecond());
        }
    }

}
