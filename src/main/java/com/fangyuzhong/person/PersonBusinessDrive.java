package com.fangyuzhong.person;

import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * hadoop驱动类
 * Created by fangyuzhong on 17-6-19.
 */
public class PersonBusinessDrive extends Configured implements Tool
{

    @Override
    public int run(String[] args) throws Exception
    {
        //环境变量设置
        File jarFile = JobUtilJar.createTempJar(JobUtilJar.outJarPath);
        ClassLoader classLoader = JobUtilJar.getClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        //获取连接hadoop集群配置，默认加载当前线程相关配置
        Configuration conf = new Configuration(true);

        //setJobName()方法命名这个Job。对Job进行合理的命名有助于更快地找到Job，
        // 以便在JobTracker和Tasktracker的页面中对其进行监视
        Job job = Job.getInstance(conf, "PersonCount Hadoop Job");
        job.setJarByClass(PersonBusinessDrive.class);

        // 环境变量调用，添加此句则可在eclipse中直接提交mapreduce任务，
        // 如果将该java文件打成jar包，需要将该句注释掉，否则在执行时反而找不到环境变量
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

        //Job处理的Map（拆分）、Combiner（中间结果合并）以及Reduce（合并）的相关处理类。
        // 这里用Reduce类来进行Map产生的中间结果合并，避免给网络数据传输产生压力。
        job.setMapperClass(PersonBusinessMapper.class);// 执行用户自定义map函数
        job.setCombinerClass(PersonBusinessReduce.class);// 对用户自定义map函数的数据处理结果进行合并，可以减少带宽消耗
        job.setReducerClass(PersonBusinessReduce.class);// 执行用户自定义reduce函数
        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PersonBusiness.class);

        System.out.println("PersonCount Hadoop Job start!");
        FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(args[0]));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[1]));

        //开始运行Job
        return(job.waitForCompletion(true)?0:-1);

    }

    /**
     * 主程序
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
        int exitCode = ToolRunner.run(new PersonBusinessDrive(), otherArgs);
        if (exitCode==0)
        {
            System.out.println("ok!");
        }
        else
        {
            System.out.println("error!");
        }
        System.exit(exitCode);
    }
}
