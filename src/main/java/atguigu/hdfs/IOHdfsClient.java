package atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.net.URI;

public class IOHdfsClient {

    // IO流上传文件
    @Test
    public void uploadFile() throws Exception{
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");
        // 获取输入流
        FileInputStream fis = new FileInputStream("src/main/resources/log4j.properties");

        // 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/user/xiaoyang/log.config"));
        // 流的拷贝
        IOUtils.copyBytes(fis, fos, configuration);
        // 关闭资源

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

    }

    @Test
    public void downloadFile() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");

        FSDataInputStream fis = fs.open(new Path("/user/xiaoyang/log.config"));

        IOUtils.copyBytes(fis, System.out, configuration);

        IOUtils.closeStream(fis);

        fs.close();
    }

    @Test
    public void putfile() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");

        FSDataOutputStream fos = fs.create(new Path("/user/xiaoyang/11.txt"));
        fos.write("love".getBytes());

        // 刷新
        fos.hflush();
        IOUtils.closeStream(fos);
        fs.close();
    }

}
