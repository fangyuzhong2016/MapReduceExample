package com.fangyuzhong.FilterDesign.TopTen;

import com.fangyuzhong.FilterDesign.TopFilterN.DistinctUserMapper;
import com.fangyuzhong.FilterDesign.TopFilterN.DistinctUserReducer;
import com.fangyuzhong.FilterDesign.TopFilterN.User;
import com.fangyuzhong.wordcount.WordCount;
import com.google.common.collect.Lists;
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
public class DistinctUserMapReduceTest
{
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private MapReduceDriver mapReduceDriver;

    @Before
    public void setUp()
    {
        DistinctUserMapper mapper = new DistinctUserMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        DistinctUserReducer reducer = new DistinctUserReducer();
        reduceDriver =ReduceDriver.newReduceDriver();
        reduceDriver.withReducer(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException
    {
        Text text = new Text("AAAA,5198612112036,M,19861211,AAAAAAA,2012-12-28 8:48:30,1");
        mapDriver.withInput(new LongWritable(),text);
        User user = new User();
        user.setId("5198612112036");
        user.setPersonName("AAAA");
        user.setBirthday("19861211");
        user.setAge(31);
        user.setGender("M");
        mapDriver.withOutput(new Text("5198612112036"),user);
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

        User user = new User();
        user.setId("5198612112036");
        user.setPersonName("AAAA");
        user.setBirthday("19861211");
        user.setAge(31);
        user.setGender("M");
        List<User> userList = Lists.newArrayList();
        userList.add(user);
        reduceDriver.withInput(new Text("5198612112036"),userList);
        reduceDriver.withOutput(user, NullWritable.get());
        reduceDriver.runTest();
        // 输出
        List<Pair> expectedOutputList = reduceDriver.getExpectedOutputs();
        for(Pair pair : expectedOutputList)
        {
            System.out.println(pair.getFirst() + " --- " + pair.getSecond());
        }
    }

    @Test
    public void testMapperAndReducer() throws IOException
    {
        Text text = new Text("AAAA,5198612112036,M,19861211,AAAAAAA,2012-12-28 8:48:30,1");
        mapReduceDriver.withInput(new LongWritable(),text);
        User user = new User();
        user.setId("5198612112036");
        user.setPersonName("AAAA");
        user.setBirthday("19861211");
        user.setAge(31);
        user.setGender("M");
        mapReduceDriver.withOutput(user,NullWritable.get());

        mapReduceDriver.runTest();
        // 输出
        List<Pair> expectedOutputList = mapReduceDriver.getExpectedOutputs();
        for(Pair pair : expectedOutputList)
        {
            System.out.println(pair.getFirst() + " ---- " + pair.getSecond());
        }
    }
}
