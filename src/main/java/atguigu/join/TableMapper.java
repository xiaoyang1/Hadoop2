package atguigu.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{

    private TableBean bean = new TableBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取输入文件类型
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        // 2 获取输入数据
        String line = value.toString();

        if(name.startsWith("order")) {
            // 3.1 切割
            String[] fields = line.split("\t");
            // 3.2 封装 bean 对象
            bean.setOrder_id(fields[0]);
            bean.setP_id(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("0");
            k.set(fields[1]);
        } else {
            String[] fields = line.split("\t");
            // 3.4 封装 bean 对象
            bean.setP_id(fields[0]);
            bean.setPname(fields[1]);
            bean.setFlag("1");
            bean.setAmount(0);
            bean.setOrder_id("");
            k.set(fields[0]);
        }

        context.write(k, bean);
    }
}
