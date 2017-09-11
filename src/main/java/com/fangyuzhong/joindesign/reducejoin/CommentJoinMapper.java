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
public class CommentJoinMapper extends Mapper<Object, Text, Text, Text>
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
        //解析获取用户的ID
        Map<String,String> paser = MRDPUtils.transformXmlToMap(value.toString());
        String userID = paser.get("UserId");
        if(userID==null) return;
        outKey.set(userID);
        //标记文章评论表B
        outValue.set("B"+value.toString());
        context.write(outKey,outValue);
    }
}
