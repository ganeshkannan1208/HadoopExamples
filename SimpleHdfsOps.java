import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class SimpleHdfsOps {
    public static void main(String[] args) throws Exception {
        // Hadoop Configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // Hadoop 2.x+ uses fs.defaultFS

        FileSystem fs = FileSystem.get(conf);

        Path dir = new Path("/user/demo");
        Path hdfsFile = new Path("/user/demo/sample.txt");
        Path localFile = new Path("sample.txt");
        Path localCopy = new Path("sample_copy.txt");

        // 1. Create directory
        fs.mkdirs(dir);
	System.out.println("Directory created");

        // 2. Upload local file → HDFS
        fs.copyFromLocalFile(localFile, hdfsFile);
	System.out.println("File Uploaded");

        // 3. Download HDFS file → local
        fs.copyToLocalFile(hdfsFile, localCopy);
	System.out.println("File Downloaded");

        // 4. Delete file and directory
        fs.delete(hdfsFile, false);
        fs.delete(dir, true);
	System.out.println("File and Directory Deleted");

        fs.close();
    }
}
