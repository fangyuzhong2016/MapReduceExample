package com.fangyuzhong.dataorganizationdesign.Binning;

import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-7-13.
 */
public class BiningDrive extends Configured implements Tool
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
        Job job = Job.getInstance(conf, "Bining Hadoop Job");
        job.setJarByClass(BiningDrive.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

        job.setNumReduceTasks(0);
        job.setMapperClass(BinningMapper.class);// 执行用户自定义map函数

        // Configure the MultipleOutputs by adding an output called "bins"
        // With the proper output format and mapper key/value pairs
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
                Text.class, NullWritable.class);

        // Enable the counters for the job
        // If there is a significant number of different named outputs, this
        // should be disabled
        MultipleOutputs.setCountersEnabled(job, true);

        System.out.println("Bining Hadoop Job start!");
        FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(args[0]));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[1]));

        //开始运行Job
        return(job.waitForCompletion(true)?0:-1);
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
        otherArgs[0] = "hdfs://192.168.2.2:8020/data/";
        // 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://192.168.2.2:8020/test_out/" + time;
        int exitCode = ToolRunner.run(new BiningDrive(), otherArgs);
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