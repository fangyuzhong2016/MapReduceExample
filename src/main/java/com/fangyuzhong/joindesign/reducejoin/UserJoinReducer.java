package com.fangyuzhong.joindesign.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by fangyuzhong on 17-9-11.
 */
public class UserJoinReducer extends Reducer<Text, Text, Text, Text>
{
    //保存Mapper输出的记录
    private  ArrayList<Text> listA = new ArrayList<Text>();
    private  ArrayList<Text> listB = new ArrayList<Text>();
    private  String joinType = null;

    @Override
    public void setup(Context context)
    {
        //从配置中获取执行的Join操作类型 inner、leftouter、rightouter、fullouter、anti
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
        executeJoinLogic(context,joinType,listA,listB);
    }

    /*
    执行连接操作
     */
    public static void executeJoinLogic(Context context,String joinType,
                                        ArrayList<Text> listA,ArrayList<Text> listB) throws IOException,
            InterruptedException
    {
        if (joinType.equalsIgnoreCase("inner"))
        {
            // 如果两个集合都不为空，执行内连接
            if (!listA.isEmpty() && !listB.isEmpty())
            {
                for (Text A : listA)
                {
                    for (Text B : listB)
                    {
                        context.write(A, B);
                    }
                }
            }
        } else if (joinType.equalsIgnoreCase("leftouter"))
        {
            // 执行左连接，遍历A表
            for (Text A : listA)
            {
                //如果B表不为空，执行A和B的连接
                if (!listB.isEmpty())
                {
                    for (Text B : listB)
                    {
                        context.write(A, B);
                    }
                } else
                {
                    // 如果B是空的，输出“”
                    context.write(A, new Text(""));
                }
            }
        } else if (joinType.equalsIgnoreCase("rightouter"))
        {
            // 执行右连接
            for (Text B : listB)
            {
                if (!listA.isEmpty())
                {
                    for (Text A : listA)
                    {
                        context.write(A, B);
                    }
                } else
                {
                    context.write(new Text(""), B);
                }
            }
        } else if (joinType.equalsIgnoreCase("fullouter"))
        {
            //执行全连接
            if (!listA.isEmpty())
            {
                for (Text A : listA)
                {
                    if (!listB.isEmpty())
                    {
                        for (Text B : listB)
                        {
                            context.write(A, B);
                        }
                    } else
                    {
                        context.write(A, new Text(""));
                    }
                }
            } else
            {
                for (Text B : listB)
                {
                    context.write(new Text(""), B);
                }
            }
        } else if (joinType.equalsIgnoreCase("anti"))
        {
            // 执行反连接
            if (listA.isEmpty() ^ listB.isEmpty())
            {
                for (Text A : listA)
                {
                    context.write(A, new Text(""));
                }

                for (Text B : listB)
                {
                    context.write(new Text(""), B);
                }
            }
        } else
        {
            throw new RuntimeException(
                    "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
        }
    }
}
