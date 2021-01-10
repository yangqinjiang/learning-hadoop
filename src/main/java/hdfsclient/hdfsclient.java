package hdfsclient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class hdfsclient {

	// 创建目录
	@Test
	public void testMkdir() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		// 配置在集群上运行
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 创建目录
		fs.mkdirs(new Path("/1108/daxian/banzhang4"));

		// 3 关闭资源
		fs.close();
		System.out.println("mkdir over");
	}

	// copy本地文件到hdfs
	@Test
	public void testCopyFromLocalFile() throws Exception {
		Configuration configuration = new Configuration();
		// 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
		configuration.set("dfs.replication", "2"); // 代码级的配置参数
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 上传文件

		fs.copyFromLocalFile(new Path("e:/flink资料下载地址.txt"), new Path("/flink.txt"));

		// 关闭资源
		fs.close();
		System.out.println("copyFromLocalFile over");

	}

	// HDFS文件下载
	@Test
	public void testCopyToLocalFile() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		// 2 执行下载操作
		// boolean delSrc 指是否将原文件删除
		// Path src 指要下载的文件路径
		// Path dst 指将文件下载到的路径
		// boolean useRawLocalFileSystem 是否开启文件校验
		fs.copyToLocalFile(false, new Path("/flink.txt"), new Path("e:/flink-testCopyToLocalFile.txt"), true);

		fs.close();
	}

	// HDFS文件夹删除
	@Test
	public void testDelete() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 执行删除
		fs.delete(new Path("/1108/"), true); // 递归删除文件夹及其内容

		// 3 关闭资源
		fs.close();
	}

	// HDFS文件名更改
	@Test
	public void testRename() throws Exception {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 修改文件名称
		fs.rename(new Path("/flink.txt"), new Path("/flink-rename.txt"));

		// 3 关闭资源
		fs.close();
	}

	// HDFS文件详情查看

	@Test
	public void testListFiles() throws Exception {
		// 1获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();

			// 输出详情
			// 文件名称
			System.out.println(status.getPath().getName());
			// 长度
			System.out.println(status.getLen());
			// 权限
			System.out.println(status.getPermission());
			// 分组
			System.out.println(status.getGroup());

			// 获取存储的块信息
			BlockLocation[] blockLocations = status.getBlockLocations();

			for (BlockLocation blockLocation : blockLocations) {

				// 获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();

				for (String host : hosts) {
					System.out.println(host);
				}
			}

			System.out.println("-----------分割线----------");
		}

		// 3 关闭资源
		fs.close();
	}

	// HDFS文件和文件夹判断
	@Test
	public void testListStatus() throws Exception {
		// 1 获取文件配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 判断是文件还是文件夹
		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		for (FileStatus fileStatus : listStatus) {

			// 如果是文件
			if (fileStatus.isFile()) {
				System.out.println("f:" + fileStatus.getPath().getName());
			} else {
				System.out.println("d:" + fileStatus.getPath().getName());
			}
		}

		// 3 关闭资源
		fs.close();
	}

	// HDFS文件上传
	// 1．需求：把本地e盘上的banhua.txt文件上传到HDFS根目录
	@Test
	public void testPutFileToHDFS() throws Exception {
		// 1 获取文件配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2,创建输入流
		FileInputStream fis = new FileInputStream(new File("e:/flink资料下载地址.txt"));

		// 3,获取输出流
		FSDataOutputStream fos = fs.create(new Path("/flink-stream-upload.txt"));

		// 4流对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	// HDFS文件下载
	@Test
	public void testGetFileFromHDFS() throws Exception {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/flink.txt"));

		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/flink-stream-download.txt"));

		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	// 定位文件读取
	// 1．需求：分块读取HDFS上的大文件，比如根目录下的/hadoop-2.7.2.tar.gz
	@Test
	public void testFileSeek1() throws Exception {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

		// 3 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("f:/hadoop-2.7.2.tar.gz.part1"));

		// 4 流的拷贝
		byte[] buf = new byte[1024];

		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(buf);
			fos.write(buf);
		}

		// 5关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}

	// 下载第二块
//	合并文件
//	在Window命令窗口中进入到目录E:\，然后执行如下命令，对数据进行合并
//	type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
//	合并完成后，将hadoop-2.7.2.tar.gz.part1重新命名为hadoop-2.7.2.tar.gz。解压发现该tar包非常完整。
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 打开输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

		// 3 定位输入数据位置
		fis.seek(1024 * 1024 * 128);

		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("f:/hadoop-2.7.2.tar.gz.part2"));

		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 6 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
}
