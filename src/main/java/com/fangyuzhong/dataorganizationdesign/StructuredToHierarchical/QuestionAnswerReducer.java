package com.fangyuzhong.dataorganizationdesign.StructuredToHierarchical;

import com.fangyuzhong.dataorganizationdesign.DataOrganUtility;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by fangyuzhong on 17-7-10.
 */
public class QuestionAnswerReducer extends Reducer<Text, Text, Text, NullWritable>
{
    private ArrayList<String> answers = new ArrayList<String>();
    private DocumentBuilderFactory dbf = DocumentBuilderFactory
            .newInstance();
    private String question = null;

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
        question = null;
        answers.clear();
        // 遍历输出内容
        for (Text t : values)
        {
            // 如果帖子是问题 存储，去掉标示
            if (t.charAt(0) == 'Q')
            {
                question = t.toString().substring(1, t.toString().length())
                        .trim();
            } else
            {
                answers.add(t.toString()
                        .substring(1, t.toString().length()).trim());
            }
        }
        if (question != null)
        {
            String postWithCommentChildren = DataOrganUtility.nestElements(dbf, question, answers);
            context.write(new Text(postWithCommentChildren),
                    NullWritable.get());
        }
    }
}
