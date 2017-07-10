package com.fangyuzhong.FilterDesign.TopTen;

import com.fangyuzhong.FilterDesign.TopFilterN.DistinctUserMapper;
import com.fangyuzhong.FilterDesign.TopFilterN.DistinctUserReducer;
import com.fangyuzhong.FilterDesign.TopFilterN.TopFilterNMapper;
import com.fangyuzhong.FilterDesign.TopFilterN.TopFilterNReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by fangyuzhong on 17-7-5.
 */
public class TopFilterNMapReduceTest
{
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private MapReduceDriver mapReduceDriver;

    @Before
    public void setUp()
    {
        TopFilterNMapper mapper = new TopFilterNMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        TopFilterNReducer reducer = new TopFilterNReducer();
        reduceDriver =ReduceDriver.newReduceDriver();
        reduceDriver.withReducer(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException
    {
        Text text1 = new Text("004955246,CANTOMANY,M,19700505,47");
        Text text2 = new Text("005111197197404013018,Q,M,19740401,43");
        Text text3 = new Text("001222519819822210209447,W,F,19821020,35");
        mapDriver.withInput(new LongWritable(),text1);
        mapDriver.withInput(new LongWritable(),text2);
        mapDriver.withInput(new LongWritable(),text3);
        mapDriver.withOutput(NullWritable.get(),text3);
        mapDriver.withOutput(NullWritable.get(),text2);
        mapDriver.withOutput(NullWritable.get(),text1);
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
    }

    @Test
    public void testMapperAndReducer() throws IOException
    {
        Text text1 = new Text("004955246,CANTOMANY,M,19700505,47");
        Text text2 = new Text("005111197197404013018,Q,M,19740401,43");
        Text text3 = new Text("001222519819822210209447,W,F,19821020,35");
        mapReduceDriver.withInput(new LongWritable(),text3);
        mapReduceDriver.withInput(new LongWritable(),text2);
        mapReduceDriver.withInput(new LongWritable(),text1);
        mapReduceDriver.withOutput(new IntWritable(1),text3);
        mapReduceDriver.withOutput(new IntWritable(2),text2);
        mapReduceDriver.withOutput(new IntWritable(3),text1);
        //mapReduceDriver.runTest();
        // 输出
        List<Pair> expectedOutputList = mapReduceDriver.getExpectedOutputs();
        for(Pair pair : expectedOutputList)
        {
            System.out.println(pair.getFirst() + " ---- " + pair.getSecond());
        }
    }
}
