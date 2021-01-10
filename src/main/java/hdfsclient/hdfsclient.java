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

	// ����Ŀ¼
	@Test
	public void testMkdir() throws IOException, InterruptedException, URISyntaxException {

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		// �����ڼ�Ⱥ������
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ����Ŀ¼
		fs.mkdirs(new Path("/1108/daxian/banzhang4"));

		// 3 �ر���Դ
		fs.close();
		System.out.println("mkdir over");
	}

	// copy�����ļ���hdfs
	@Test
	public void testCopyFromLocalFile() throws Exception {
		Configuration configuration = new Configuration();
		// �������ȼ����򣺣�1���ͻ��˴��������õ�ֵ >��2��ClassPath�µ��û��Զ��������ļ� >��3��Ȼ���Ƿ�������Ĭ������
		configuration.set("dfs.replication", "2"); // ���뼶�����ò���
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// �ϴ��ļ�

		fs.copyFromLocalFile(new Path("e:/flink�������ص�ַ.txt"), new Path("/flink.txt"));

		// �ر���Դ
		fs.close();
		System.out.println("copyFromLocalFile over");

	}

	// HDFS�ļ�����
	@Test
	public void testCopyToLocalFile() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		// 2 ִ�����ز���
		// boolean delSrc ָ�Ƿ�ԭ�ļ�ɾ��
		// Path src ָҪ���ص��ļ�·��
		// Path dst ָ���ļ����ص���·��
		// boolean useRawLocalFileSystem �Ƿ����ļ�У��
		fs.copyToLocalFile(false, new Path("/flink.txt"), new Path("e:/flink-testCopyToLocalFile.txt"), true);

		fs.close();
	}

	// HDFS�ļ���ɾ��
	@Test
	public void testDelete() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ִ��ɾ��
		fs.delete(new Path("/1108/"), true); // �ݹ�ɾ���ļ��м�������

		// 3 �ر���Դ
		fs.close();
	}

	// HDFS�ļ�������
	@Test
	public void testRename() throws Exception {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 �޸��ļ�����
		fs.rename(new Path("/flink.txt"), new Path("/flink-rename.txt"));

		// 3 �ر���Դ
		fs.close();
	}

	// HDFS�ļ�����鿴

	@Test
	public void testListFiles() throws Exception {
		// 1��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ��ȡ�ļ�����
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();

			// �������
			// �ļ�����
			System.out.println(status.getPath().getName());
			// ����
			System.out.println(status.getLen());
			// Ȩ��
			System.out.println(status.getPermission());
			// ����
			System.out.println(status.getGroup());

			// ��ȡ�洢�Ŀ���Ϣ
			BlockLocation[] blockLocations = status.getBlockLocations();

			for (BlockLocation blockLocation : blockLocations) {

				// ��ȡ��洢�������ڵ�
				String[] hosts = blockLocation.getHosts();

				for (String host : hosts) {
					System.out.println(host);
				}
			}

			System.out.println("-----------�ָ���----------");
		}

		// 3 �ر���Դ
		fs.close();
	}

	// HDFS�ļ����ļ����ж�
	@Test
	public void testListStatus() throws Exception {
		// 1 ��ȡ�ļ�������Ϣ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 �ж����ļ������ļ���
		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		for (FileStatus fileStatus : listStatus) {

			// ������ļ�
			if (fileStatus.isFile()) {
				System.out.println("f:" + fileStatus.getPath().getName());
			} else {
				System.out.println("d:" + fileStatus.getPath().getName());
			}
		}

		// 3 �ر���Դ
		fs.close();
	}

	// HDFS�ļ��ϴ�
	// 1�����󣺰ѱ���e���ϵ�banhua.txt�ļ��ϴ���HDFS��Ŀ¼
	@Test
	public void testPutFileToHDFS() throws Exception {
		// 1 ��ȡ�ļ�������Ϣ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2,����������
		FileInputStream fis = new FileInputStream(new File("e:/flink�������ص�ַ.txt"));

		// 3,��ȡ�����
		FSDataOutputStream fos = fs.create(new Path("/flink-stream-upload.txt"));

		// 4���Կ�
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	// HDFS�ļ�����
	@Test
	public void testGetFileFromHDFS() throws Exception {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/flink.txt"));

		// 3 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("e:/flink-stream-download.txt"));

		// 4 ���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	// ��λ�ļ���ȡ
	// 1�����󣺷ֿ��ȡHDFS�ϵĴ��ļ��������Ŀ¼�µ�/hadoop-2.7.2.tar.gz
	@Test
	public void testFileSeek1() throws Exception {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

		// 3 ���������
		FileOutputStream fos = new FileOutputStream(new File("f:/hadoop-2.7.2.tar.gz.part1"));

		// 4 ���Ŀ���
		byte[] buf = new byte[1024];

		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(buf);
			fos.write(buf);
		}

		// 5�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}

	// ���صڶ���
//	�ϲ��ļ�
//	��Window������н��뵽Ŀ¼E:\��Ȼ��ִ��������������ݽ��кϲ�
//	type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
//	�ϲ���ɺ󣬽�hadoop-2.7.2.tar.gz.part1��������Ϊhadoop-2.7.2.tar.gz����ѹ���ָ�tar���ǳ�������
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ��������
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

		// 3 ��λ��������λ��
		fis.seek(1024 * 1024 * 128);

		// 4 ���������
		FileOutputStream fos = new FileOutputStream(new File("f:/hadoop-2.7.2.tar.gz.part2"));

		// 5 ���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);

		// 6 �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
}
