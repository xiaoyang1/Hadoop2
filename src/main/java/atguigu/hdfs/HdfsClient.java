package atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

public class HdfsClient {
    // 上传文件
    public static void main(String[] args) throws Exception{

        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");

        if(fs == null){
            System.out.println("fs  null");
            return;
        }

        // 上传文件
        fs.copyFromLocalFile(new Path("./hello.txt"), new Path("/user/xiaoyang/hello.txt"));

        // 关闭资源
        fs.close();
    }


    @Test
    public void upload() throws Exception{
        // 获取文件系统
        Configuration configuration = new Configuration();
        // 配置副本数, 代码优先级高于配置文件
        configuration.set("dfs.replication", "1");

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");

        // 上传文件
        fs.copyFromLocalFile(new Path("./hello.txt"), new Path("/user/xiaoyang/hello.txt"));

        fs.close();
    }

    @Test
    public void testListFiles() throws Exception{
        Configuration configuration = new Configuration();
        // 获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 获取文件名称
            System.out.println(status.getPath().getName());
            // 获取长度
            System.out.println(status.getLen());
            // 获取权限
            System.out.println(status.getPermission());
            // 获取 z 组
            System.out.println(status.getGroup());

            // 获取存储块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for(BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();

                for(String host : hosts){
                    System.out.println(host);
                }
            }
            System.out.println("-----------------------------------------");
        }

        fs.close();
    }

    @Test
    public void testListStatus() throws Exception{
        Configuration configuration = new Configuration();
        // 获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for(FileStatus status : listStatus){
            // if file
            if(status.isFile()){
                System.out.println("f: " + status.getPath().getName());
            } else {
                System.out.println("d: " + status.getPath().getName());
            }

        }

        fs.close();
    }

}
