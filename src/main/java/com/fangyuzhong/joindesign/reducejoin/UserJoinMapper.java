package com.fangyuzhong.joindesign.reducejoin;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/**
 * Created by fangyuzhong on 17-9-11.
 */
public class UserJoinMapper extends Mapper<Object, Text, Text, Text>
{

    private Text outKey = new Text();
    private Text outValue = new Text();

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
        //解析用户的ID
        Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
        String userId = parsed.get("UserId");
        if(userId==null) return;
        outKey.set(userId);
        //标记该连接属于的数据集名称，用户属于A表
        outValue.set("A"+value.toString());
        context.write(outKey,outValue);
    }
}
