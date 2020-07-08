package exec;

import exec.FileToBlock;
import exec.JDBCUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Objects;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 15:55
 */
public class Main2 {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        String path = Objects.requireNonNull(FileToBlock.class.getClassLoader().getResource("data.txt")).getPath();
        try (
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(path)));
                BufferedReader in = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8), 10 * 1024 * 1024)
        ) {
            Connection conn = null;
            conn = JDBCUtils.getConnection();
            conn.setAutoCommit(false);
            PreparedStatement ps = null;
            ps = conn.prepareStatement("INSERT INTO info (C0,C1,C2,C3) VALUES(?,?,?,?)");
            int count = 0;
            while (in.ready()) {
                String line = in.readLine();
                try {
                    String[] arr = line.split(",");        //将读取的每一行以 , 号分割成数组
                    if (arr.length < 3) continue;            //arr数组长度大于3才是一条完整的数据
                    ps.setString(1, null);
                    ps.setString(2, arr[0]);
                    ps.setString(3, arr[1]);
                    ps.setString(4, arr[2]);
                    ps.addBatch();

                    if (++count % 15 == 0) {
                        ps.executeBatch(); //5
                        //手动提交一次事务
                        conn.commit();
                        // 清空批
                        ps.clearBatch();

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                ps.executeBatch();
                conn.commit();
                ps.clearBatch();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000 + "s");
    }
}
