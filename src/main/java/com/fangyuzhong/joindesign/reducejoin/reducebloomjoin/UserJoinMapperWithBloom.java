package com.fangyuzhong.joindesign.reducejoin.reducebloomjoin;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/**
 * Created by fangyuzhong on 17-9-11.
 */
public class UserJoinMapperWithBloom extends Mapper<Object, Text, Text, Text>
{

    private Text outkey = new Text();
    private Text outvalue = new Text();
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

        String userId = parsed.get("Id");
        String reputation = parsed.get("Reputation");

        if (userId == null || reputation == null)
        {
            return;
        }
        //进行用户数据过滤
        if (Integer.parseInt(reputation) > 1500)
        {
            outkey.set(parsed.get("Id"));
            outvalue.set("A" + value.toString());
            context.write(outkey, outvalue);
        }
    }
}
