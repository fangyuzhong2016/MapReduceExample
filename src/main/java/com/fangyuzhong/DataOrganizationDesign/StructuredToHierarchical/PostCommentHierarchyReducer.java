package com.fangyuzhong.DataOrganizationDesign.StructuredToHierarchical;


import com.fangyuzhong.DataOrganizationDesign.DataOrganUtility;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 评论帖子Reduce类：创建有层次的XML对象。判断map输出的value，有标示P的表明该value是帖子，
 * 去掉P，将其存储到字符串中，否则表明是评论，去掉C，存储到数组中。然后，序列化为XML对象输出。<p>
 * Created by fangyuzhong on 17-7-10.
 */
public class PostCommentHierarchyReducer extends Reducer<Text, Text, Text, NullWritable>
{
    private ArrayList<String> comments = new ArrayList<String>();//存储评论
    private DocumentBuilderFactory dbf = DocumentBuilderFactory
            .newInstance();//XML创建对象工厂
    private String post = null;//存储帖子内容

    /**
     * reduce函数
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws InterruptedException, IOException
    {
        post = null;
        comments.clear();

        // 遍历输出的value，获取内容
        for (Text t : values)
        {
            // 如果该value是帖子，存储起来，并去掉标示P
            if (t.charAt(0) == 'P')
            {
                post = t.toString().substring(1, t.toString().length())
                        .trim();
            } else
            {
                //否则标示是评论，去掉前面的标示C，存储到数组中（一个帖子会有多个评论）
                comments.add(t.toString()
                        .substring(1, t.toString().length()).trim());
            }
        }
        // 帖子内容不为空时输出
        if (post != null)
        {
            // 根据帖子和评论，创建层次结构XML 字符串
            String postWithCommentChildren = DataOrganUtility.nestElements(dbf, post, comments);
            // 输出
            context.write(new Text(postWithCommentChildren), NullWritable.get());
        }
    }
}
