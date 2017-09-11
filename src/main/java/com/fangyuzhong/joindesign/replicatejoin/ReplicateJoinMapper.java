package com.fangyuzhong.joindesign.replicatejoin;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * 复制连接模式的Mapper
 * Created by fangyuzhong on 17-9-11.
 */
public class ReplicateJoinMapper extends Mapper<Object, Text, Text, Text>
{

    private HashMap<String, String> userIdToInfo = new HashMap<String, String>();
    private Text outvalue = new Text();
    private String joinType = null;

    /**
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException
    {
        try
        {
            //从分布式缓存中拿到小数据集
            Path[] files = context.getLocalCacheFiles();
            if (files == null || files.length == 0)
            {
                throw new RuntimeException(
                        "User information is not set in DistributedCache");
            }

            // 遍历所有小数据集解析
            for (Path p : files)
            {
                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(
                                new GZIPInputStream(new FileInputStream(
                                        new File(p.toString())))));
                String line;
                // 解析每个文件的行
                while ((line = rdr.readLine()) != null)
                {

                    // 读取用户的ID
                    Map<String, String> parsed = MRDPUtils
                            .transformXmlToMap(line);
                    String userId = parsed.get("Id");

                    if (userId != null)
                    {
                        // 把用户的ID连同整个记录写入hashmap
                        userIdToInfo.put(userId, line);
                    }
                }
            }

        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        //获取连接的类型
        joinType = context.getConfiguration().get("join.type");
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
        // 解析文章的评论数据
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                .toString());

        //获取用户的ID
        String userId = parsed.get("UserId");

        if (userId == null)
        {
            return;
        }

        //拿文章评论的用户ID查找HashMap
        String userInformation = userIdToInfo.get(userId);

        // 如果用户信息不为空，输出
        if (userInformation != null)
        {
            outvalue.set(userInformation);
            context.write(value, outvalue);
        } else if (joinType.equalsIgnoreCase("leftouter"))
        {
            //判断操作类型，如果是左连接，输出空，否则忽略
            context.write(value, new Text(""));
        }
    }
}
