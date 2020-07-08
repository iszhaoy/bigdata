package exec;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.Callable;

public class MapperRunner implements Callable<Long> {

    private long nums;
    private FileSplit fs;

    public MapperRunner(FileSplit fs) {
        this.fs = fs;
    }

    public Long call() {
        try {
            File file = new File(fs.getPath());
            long start = fs.getStart();
            long end = start + fs.getLength();

            FileChannel fc = new FileInputStream(
                    file).getChannel();
            if (start != 0) {
                long headPosition = start;
                while (true) {
                    ByteBuffer buf = ByteBuffer.allocate(1);
                    fc.position(headPosition);
                    fc.read(buf);
                    if ("\n".equals(new String(buf.array()))) {
                        headPosition++;
                        start = headPosition;
                        break;
                    } else {
                        headPosition--;
                    }
                }
            }
            //end不为文件的结尾
            if (end != file.length()) {
                long endPosition = end;
                while (true) {
                    ByteBuffer buf = ByteBuffer.allocate(1);
                    fc.position(endPosition);
                    fc.read(buf);
                    if ("\n".equals(new String(buf.array()))) {
                        end = endPosition;
                        break;
                    } else {
                        endPosition--;
                    }
                }
            }
            fc.position(start);
            ByteBuffer splitBuf = ByteBuffer.allocate(
                    (int) (end - start));
            fc.read(splitBuf);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            new ByteArrayInputStream(splitBuf.array())));
            String line = null;
            Connection conn = null;
            PreparedStatement ps = null;
            try {
                conn = JDBCUtils.getConnection();
                conn.setAutoCommit(false);
                ps = conn.prepareStatement("INSERT INTO info (C0,C1,C2,C3) VALUES(?,?,?,?)");
                while ((line = reader.readLine()) != null) {
//                    System.out.println(line);
                    String[] arr = line.split(",");        //将读取的每一行以 , 号分割成数组
                    if (arr.length < 3) continue;            //arr数组长度大于3才是一条完整的数据
                    ps.setString(1, null);
                    ps.setString(2, arr[0]);
                    ps.setString(3, arr[1]);
                    ps.setString(4, arr[2]);
                    ps.addBatch();
                    if (++nums % GlobalEnv.BATCH_SIZE == 0) {
                        ps.executeBatch(); //5
                        //手动提交一次事务
                        conn.commit();
                        // 清空批
                        ps.clearBatch();
                    }

                }

                ps.executeBatch();
                conn.commit();
                ps.clearBatch();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException();
            } finally {
                ps.close();
                conn.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + "处理的切片信息：" + fs);
        System.out.println(Thread.currentThread().getName() + "处理了：" + nums + "条");
        return nums;
    }
}
