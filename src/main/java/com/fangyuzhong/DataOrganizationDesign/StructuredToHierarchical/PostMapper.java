package com.fangyuzhong.DataOrganizationDesign.StructuredToHierarchical;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * 帖子类
 * Created by fangyuzhong on 17-7-10.
 */
public class PostMapper extends Mapper<Object, Text, Text, Text>
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
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
        String postId = parsed.get("Id");
        if (postId == null) return;
        outKey.set(postId);
        outValue.set("P" + value.toString());
        context.write(outKey, outValue);
    }
}
