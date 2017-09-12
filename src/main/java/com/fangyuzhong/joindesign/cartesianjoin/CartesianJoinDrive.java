package com.fangyuzhong.joindesign.cartesianjoin;

import com.fangyuzhong.joindesign.compositejoin.CompositeJoinDrive;
import com.fangyuzhong.joindesign.reducejoin.CommentJoinMapper;
import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-9-12.
 */
public class CartesianJoinDrive extends Configured implements Tool
{
    /**
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception
    {
        File jarFile = JobUtilJar.createTempJar(JobUtilJar.outJarPath);
        ClassLoader classLoader = JobUtilJar.getClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        //获取连接hadoop集群配置，默认加载当前线程相关配置
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf, "Cartesian-Join Hadoop Job");
        job.setJarByClass(CartesianJoinDrive.class);

        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        //设置笛卡尔积连接的Mapper类
        ((JobConf) job.getConfiguration()).setMapperClass(CartesianMapper.class);
        //没有Reduce操作，设置为0
        job.setNumReduceTasks(0);

        //设置笛卡尔积连接分区输入类
        ((JobConf) job.getConfiguration()).setInputFormat(CartesianInputFormat.class);
        CartesianInputFormat.setLeftInputInfo( ((JobConf) job.getConfiguration()), TextInputFormat.class,
                args[0]);
        CartesianInputFormat.setRightInputInfo( ((JobConf) job.getConfiguration()), TextInputFormat.class,
                args[0]);

        //设置输出路径
        TextOutputFormat.setOutputPath( ((JobConf) job.getConfiguration()), new Path(args[1]));

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("Cartesian-Join Hadoop Job start!");

        //开始运行Job
        return (job.waitForCompletion(true) ? 0 : -1);
    }

    /**
     * 主程序
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        String[] otherArgs = new String[2];
        // 计算原文件目录，需提前在里面存入文件
        otherArgs[0] = "hdfs://192.168.2.2:8020/persondata/";
        // 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://192.168.2.2:8020/test_out/" + time;
        int exitCode = ToolRunner.run(new CartesianJoinDrive(), otherArgs);
        if (exitCode == 0)
        {
            System.out.println("ok!");
        } else
        {
            System.out.println("error!");
        }
        System.exit(exitCode);
    }
}
