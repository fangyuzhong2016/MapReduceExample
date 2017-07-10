package com.fangyuzhong.DataOrganizationDesign.StructuredToHierarchical;

import com.fangyuzhong.DataOrganizationDesign.DataOrganUtility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;

/**
 * Created by fangyuzhong on 17-7-10.
 */
public class QuestionAnswerMapper extends Mapper<Object, Text, Text, Text>
{
    private DocumentBuilderFactory dbf = DocumentBuilderFactory
            .newInstance();
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
        // 解析 帖子/评论 XML 的元素
        Element post = DataOrganUtility.getXmlElementFromString(dbf, value.toString());

        int postType = Integer.parseInt(post.getAttribute("PostTypeId"));

        // 如果帖子类型 postType 是 1, 那该帖子属于问题
        if (postType == 1)
        {
            outkey.set(post.getAttribute("Id"));
            outvalue.set("Q" + value.toString());
        } else
        {
            //否则属于回答
            outkey.set(post.getAttribute("ParentId"));
            outvalue.set("A" + value.toString());
        }

        context.write(outkey, outvalue);
    }
}
