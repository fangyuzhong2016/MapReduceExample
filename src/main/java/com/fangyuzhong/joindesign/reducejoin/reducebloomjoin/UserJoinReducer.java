package com.fangyuzhong.joindesign.reducejoin.reducebloomjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by fangyuzhong on 17-9-11.
 */
public class UserJoinReducer extends Reducer<Text, Text, Text, Text>
{
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    public void setup(Context context) {
        // Get the type of join from our configuration
        joinType = context.getConfiguration().get("join.type");
    }
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
        // 清空保存的临时集合
        listA.clear();
        listB.clear();

        //遍历map 的输出，按照标记，分别存储到不同的临时集合中
        for (Text t : values)
        {
            if (t.charAt(0) == 'A')
            {
                listA.add(new Text(t.toString().substring(1)));
            } else if (t.charAt(0) == 'B')
            {
                listB.add(new Text(t.toString().substring(1)));
            }
        }

        // 执行连接操作
        com.fangyuzhong.joindesign.reducejoin.UserJoinReducer.executeJoinLogic(context,joinType,listA,listB);
    }
}
