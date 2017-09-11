package com.fangyuzhong.joindesign.reducejoin.reducebloomjoin;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.*;
import java.util.Map;

/**
 * Created by fangyuzhong on 17-9-11.
 */
public class CommentJoinMapperWithBloom extends Mapper<Object, Text, Text, Text>
{

    private BloomFilter bfilter = new BloomFilter();
    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    public void setup(Context context)
    {
        try
        {
            //从分布式缓存中获取数据
            Path[] files = context.getLocalCacheFiles();
            if (files.length != 0)
            {
                DataInputStream strm = new DataInputStream(
                        new FileInputStream(new File(
                                files[0].toString())));
                bfilter.readFields(strm);
            } else
            {
                throw new RuntimeException(
                        "Bloom filter not set in DistributedCache");
            }
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
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
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                .toString());

        String userId = parsed.get("UserId");

        if (userId == null)
        {
            return;
        }

        if (bfilter.membershipTest(new Key(userId.getBytes())))
        {
            outkey.set(userId);
            outvalue.set("B" + value.toString());
            context.write(outkey, outvalue);
        }
    }
}
