package com.fangyuzhong.FilterDesign.TopFilterN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Created by fangyuzhong on 17-7-4.
 */
public class TopFilterNMapper extends Mapper<Object, Text, NullWritable, User>
{

    private SortedMap<Integer,User> top10Person = new TreeMap();
    private int N =10;//默认前10个
    private User outUser = new User();

    /**
     * 获取前N的值，由作业驱动器设置
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        N = context.getConfiguration().getInt("top.n",10);
    }

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
        if(valueSplits.length>=5)
        {
            String id = valueSplits[0];
            String name = valueSplits[1];
            String gender = valueSplits[2];
            String birthday = valueSplits[3];
            String age = valueSplits[4];
            int personAge = 0;
            try
            {
                personAge = Integer.parseInt(age);
            }
            catch (Exception ex)
            {

            }
//            if(personAge==0) return;
            outUser.setId(id);
            outUser.setPersonName(name);
            outUser.setGender(gender);
            outUser.setBirthday(birthday);
            outUser.setAge(personAge);
            //加入Map集合
            top10Person.put(personAge,outUser);
            if(top10Person.size()>N)
            {
                top10Person.remove(top10Person.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        super.cleanup(context);
        for(User var:top10Person.values())
        {
            context.write(NullWritable.get(),var);
        }
    }
}
