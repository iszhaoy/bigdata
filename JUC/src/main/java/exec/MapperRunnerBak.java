package exec;

import forkjoin.JDBCUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 15:02
 */

class Count {
    volatile int count;
}

public class MapperRunnerBak implements Runnable {

    private Count count;
    private long nums;
    private FileSplit fs;

    public MapperRunnerBak(FileSplit fs, Count count) {
        this.fs = fs;
        this.count = count;
    }

    public void run() {
        try {
            File file = new File(fs.getPath().toString());
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
                ps = conn.prepareStatement("INSERT INTO INFO (C0,C1,C2,C3) VALUES(?,?,?,?)");
                while ((line = reader.readLine()) != null) {
                    String[] arr = line.split(",");        //将读取的每一行以 , 号分割成数组
                    if (arr.length < 3) continue;            //arr数组长度大于3才是一条完整的数据
                    ps.setString(1, null);
                    ps.setString(2, arr[0]);
                    ps.setString(3, arr[1]);
                    ps.setString(4, arr[2]);
                    ps.addBatch();

                    synchronized (MapperRunnerBak.class) {
                        count.count++;
                        nums++;
                        if (++nums % 15 == 0) {
                            ps.executeBatch(); //5
                            //手动提交一次事务
                            conn.commit();
                            // 清空批
                            ps.clearBatch();

                        }
                    }

//                    System.out.println(line);
                }

                ps.executeBatch();
                conn.commit();
                ps.clearBatch();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + "处理的切片信息：" + fs);
        System.out.println(Thread.currentThread().getName() + "处理了：" + nums + "条");
    }
}
