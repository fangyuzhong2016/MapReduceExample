package com.fangyuzhong.dataorganizationdesign.StructuredToHierarchical;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * 评论类
 * Created by fangyuzhong on 17-7-10.
 */
public class CommentsMapper extends Mapper<Object, Text, Text, Text>
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
        // 存储解析的内容Map
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                .toString());
        String postId = parsed.get("PostId");//评论对应的帖子ID
        if (postId == null)
        {
            return;
        }
        //使用外健ID
        outkey.set(postId);
        // 标记该内容为评论，输出
        outvalue.set("C" + value.toString());
        context.write(outkey, outvalue);
    }

}
