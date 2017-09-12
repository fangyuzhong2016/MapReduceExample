package com.fangyuzhong.joindesign.compositejoin;

import com.fangyuzhong.joindesign.reducejoin.CommentJoinMapper;
import com.fangyuzhong.joindesign.reducejoin.ReduceSideJoinDriver;
import com.fangyuzhong.joindesign.reducejoin.UserJoinMapper;
import com.fangyuzhong.joindesign.reducejoin.UserJoinReducer;
import com.fangyuzhong.utility.JobUtilJar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-9-12.
 */
public class CompositeJoinDrive extends Configured implements Tool
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
        Job job = Job.getInstance(conf, "Composite-Join Hadoop Job");
        job.setJarByClass(CompositeJoinDrive.class);

        //设置相关输入的路径和连接操作的类型
        Path userPath = new Path(args[0]);//用户信息的路径
        Path commentPath = new Path(args[1]);//文章评论信息的路径
        Path outputDir = new Path(args[2]);//输出的目录路径
        String joinType = args[3];//获取配置的连接操作类型
        if (!(joinType.equalsIgnoreCase("inner") || joinType
                .equalsIgnoreCase("outer")))
        {
            System.err.println("Join type not set to inner or outer");
            System.exit(2);
        }
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        //设置复合连接的Mapper类
        job.setMapperClass(CommentJoinMapper.class);
        //没有Reduce操作，设置为0
        job.setNumReduceTasks(0);

        //使用MR自帶的輸入格式CompositeInputFormat作为Job的输入
        //CompositeInputFormat类，将会解析所有的输入文件并输出记录到Mapper
        ((JobConf) job.getConfiguration()).setInputFormat(CompositeInputFormat.class);

        // 复合输入格式连接表达式将设置如何读取记录以及以什么输入格式。
        conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType,
                KeyValueTextInputFormat.class, userPath, commentPath));

        //设置输出路径
        TextOutputFormat.setOutputPath( ((JobConf) job.getConfiguration()), outputDir);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("Reduce-Join Hadoop Job start!");

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
        otherArgs[0] = "hdfs://192.168.2.2:8020/user/";
        otherArgs[1] = "hdfs://192.168.2.2:8020/Comment/";
        // 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[2] = "hdfs://192.168.2.2:8020/test_out/" + time;
        otherArgs[3]="inner";
        int exitCode = ToolRunner.run(new CompositeJoinDrive(), otherArgs);
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
