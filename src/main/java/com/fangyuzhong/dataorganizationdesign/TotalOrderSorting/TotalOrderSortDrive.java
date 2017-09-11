package com.fangyuzhong.dataorganizationdesign.TotalOrderSorting;

import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-7-13.
 */
public class TotalOrderSortDrive extends Configured implements Tool
{
    /**
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception
    {
        //环境变量设置
        File jarFile = JobUtilJar.createTempJar(JobUtilJar.outJarPath);
        ClassLoader classLoader = JobUtilJar.getClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        //获取连接hadoop集群配置，默认加载当前线程相关配置
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf, "全局排序第一阶段  Hadoop Job");
        job.setJarByClass(TotalOrderSortDrive.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

        //解析输入处理
        Path inputPath = new Path(args[0]);
        Path partitionFile = new Path(args[1] + "_partitions.lst");
        Path outputStage = new Path(args[1] + "_staging");
        Path outputOrder = new Path(args[1]);
        double sampleRate = Double.parseDouble(args[2]);

        //清除相关文件
        FileSystem.get(new Configuration()).delete(outputOrder, true);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        FileSystem.get(new Configuration()).delete(partitionFile, true);

        //设置MapperClass
        job.setMapperClass(TotalOrderSortingMapper.class);
        job.setNumReduceTasks(0);

        //设置Mapper输出的格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置Job输入
        TextInputFormat.setInputPaths((JobConf) job.getConfiguration(), inputPath);
        //设置Job输出
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputStage);

        System.out.println("全局排序第一阶段 Hadoop Job start!");

        //开始运行Job
        int code = (job.waitForCompletion(true) ? 0 : -1);
        if (code == 0)
        {
            System.out.println("全局排序第一阶段 Hadoop Job 结束!");
            Job orderJob = Job.getInstance(conf, "全局排序第二阶段 Hadoop Job");
            orderJob.setJarByClass(TotalOrderSortDrive.class);
            //再次设置依赖
            ((JobConf) orderJob.getConfiguration()).setJar(jarFile.toString());
            // 使用默认的Mapper输出二进制Key/Value
            orderJob.setMapperClass(Mapper.class);
            orderJob.setReducerClass(TotalOrderSortValueReducer.class);
            // 将reduce任务的数量设置为正在排序的数据量的适当数量
            orderJob.setNumReduceTasks(10);
            // 使用Hadoop的全局排序类 进行分区
            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            // 设置分区文件输出
            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);
            //设置排序Job的map输出格式
            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);
            // 将输入设置为上一个作业的输出
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
            // 设置全局排序最终的结果输出
            TextOutputFormat.setOutputPath((JobConf) orderJob.getConfiguration(), outputOrder);
            // 将分隔符设置为空字符串
            orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");
            // 使用InputSampler通过上一个作业的输出，对其进行采样，并创建分区文件
            InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(sampleRate, 10000));
            System.out.println("全局排序第二阶段 Hadoop Job 开始!");
            // 提交作业运行
            return orderJob.waitForCompletion(true) ? 0 : 2;
        }
        return code;
    }

    /**
     * 主程序
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        String[] otherArgs = new String[3];
        // 计算原文件目录，需提前在里面存入文件
        otherArgs[0] = "hdfs://192.168.2.2:8020/data/";
        // 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://192.168.2.2:8020/test_out/" + time;
        otherArgs[2] = "0.1";//因子
        int exitCode = ToolRunner.run(new TotalOrderSortDrive(), otherArgs);
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
