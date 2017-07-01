package com.fangyuzhong.FilterDesign.BloomFilter;

import com.fangyuzhong.InvertedIndex.*;
import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-6-30.
 */
public class BloomFilterDrive extends Configured implements Tool
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
        Job job = Job.getInstance(conf, "BloomFilter Hadoop Job");
        job.setJarByClass(BloomFilterDrive.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());


        //热点列表数据目录
        String bpath="hdfs://192.168.2.2:8020/input/bloom.bin";
        //添加到共享cache里,注意地址的软链接
        String inPathLink =bpath+"#"+"bloom.bin";
        job.addCacheFile(new URI(inPathLink));


        job.setMapperClass(BloomFilterMapper.class);// 执行用户自定义map函数
        job.setReducerClass(BloomFilterReducer.class);// 执行用户自定义reduce函数

        //设置一下Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.out.println("BloomFilter Hadoop Job start!");
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
        otherArgs[0] = "hdfs://192.168.2.2:8020/persondata/";
        // 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://192.168.2.2:8020/test_out/" + time;
        int exitCode = ToolRunner.run(new BloomFilterDrive(), otherArgs);
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
