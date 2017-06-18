package com.fangyuzhong.person;

import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-6-12.
 */
public class PersonBusinessMain
{
    public static void main(String[] args) throws Exception
    {

        /**
         * 环境变量配置
         */
        File jarFile = JobUtilJar.createTempJar("bin");
        ClassLoader classLoader = JobUtilJar.getClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        /**
         * 连接hadoop集群配置
         */
        Configuration conf = new Configuration(true);

        String[] otherArgs = new String[2];
        otherArgs[0] = "hdfs://192.168.2.2:8020/input/";// 计算原文件目录，需提前在里面存入文件
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://192.168.2.2:8020/test_out/" + time;// 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签

      /*
       * setJobName()方法命名这个Job。对Job进行合理的命名有助于更快地找到Job，
       * 以便在JobTracker和Tasktracker的页面中对其进行监视
       */
        Job job = Job.getInstance(conf, "PersonCount Hadoop Job");
        job.setJarByClass(PersonBusinessMain.class);

        // 环境变量调用，添加此句则可在eclipse中直接提交mapreduce任务，
        // 如果将该java文件打成jar包，需要将该句注释掉，否则在执行时反而找不到环境变量
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

      /*
       * Job处理的Map（拆分）、Combiner（中间结果合并）以及Reduce（合并）的相关处理类。
       * 这里用Reduce类来进行Map产生的中间结果合并，避免给网络数据传输产生压力。
       */
        job.setMapperClass(PersonBusinessMapper.class);// 执行用户自定义map函数
        //job.setCombinerClass(WordCount.IntSumReducer.class);// 对用户自定义map函数的数据处理结果进行合并，可以减少带宽消耗
        job.setReducerClass(PersonBusinessReduce.class);// 执行用户自定义reduce函数

      /*
       * 接着设置Job输出结果<key,value>的中key和value数据类型，因为结果是<单词,个数>，
       * 所以key设置为"Text"类型，相当于Java中String类型
       * 。Value设置为"IntWritable"，相当于Java中的int类型。
       */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PersonBusiness.class);

      /*
       * 加载输入文件夹或文件路径，即输入数据的路径
       * 将输入的文件数据分割成一个个的split，并将这些split分拆成<key,value>对作为后面用户自定义map函数的输入
       * 其中，每个split文件的大小尽量小于hdfs的文件块大小
       * （默认64M），否则该split会从其它机器获取超过hdfs块大小的剩余部分数据，这样就会产生网络带宽造成计算速度影响
       * 默认使用TextInputFormat类型，即输入数据形式为文本类型数据文件
       */
        System.out.println("PersonCount Hadoop Job start!");
        FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(otherArgs[0]));

      /*
       * 设置输出文件路径 默认使用TextOutputFormat类型，即输出数据形式为文本类型文件，字段间默认以制表符隔开
       */
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(otherArgs[1]));

      /*
       * 开始运行上面的设置和算法
       */
        if (job.waitForCompletion(true))
        {
            System.out.println("ok!");
        } else
        {
            System.out.println("error!");
            System.exit(0);
        }
    }
}
