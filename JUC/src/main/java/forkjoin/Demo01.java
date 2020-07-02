package forkjoin;

import com.oracle.webservices.internal.api.databinding.DatabindingMode;

import javax.sound.sampled.Line;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class Demo01 {
    public static void main(String[] args) {

        String path = Demo01.class.getClassLoader().getResource("data.txt").getPath();

        long start = System.currentTimeMillis();
        int i = 0;
        int count = 0;
        List<Infos> infosList = new ArrayList<>();
        try (
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(path)));
                BufferedReader in = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8), 10 * 1024 * 1024)
        ) {
            while (in.ready()) {
                String line = in.readLine();
                String[] arr = line.split(",");        //将读取的每一行以 , 号分割成数组
                if (arr.length < 3) continue;            //arr数组长度大于3才是一条完整的数据
                Infos infos = new Infos(arr[0], arr[1], arr[2]);
                infosList.add(infos);
                int throld = 1000;
                if (++count % throld == 0) {
                    System.out.println("\n总共读取Txt文件中" + i + "条数据");
                    // 一个批次,forkjoin 插入数据库
                    ForkJoinPool pool = new ForkJoinPool();
                    ForkJoinTask<Integer> taskFuture = pool.submit(new MyForkJoinTask(infosList));

                    Integer result = taskFuture.get();
                    System.out.println(result);
                }
            }

            long end = System.currentTimeMillis();
            System.out.println((end - start) / 1000 + "s");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MyForkJoinTask extends RecursiveTask<Integer> {
    private static final Integer MAX = 200;

    private List<Infos> infosList;

    public MyForkJoinTask(List<Infos> infosList) {
        this.infosList = infosList;
    }

    @Override
    protected Integer compute() {
        if (infosList.size() <= MAX) {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
                conn = JDBCUtils.getConn();
                conn.setAutoCommit(false);
                ps = conn.prepareStatement("insert  into table (c1,c2,c3) values(?,?,?)");
                for (Infos infos : infosList) {
                    ps.setString(1, infos.getC1());
                    ps.setString(2, infos.getC2());
                    ps.setString(3, infos.getC3());
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
                ps.clearBatch();
                return infosList.size();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException();
            }

        } else {
            int mid = this.infosList.size() / 2;
            MyForkJoinTask subTask1 = new MyForkJoinTask(infosList.subList(0, mid));
            subTask1.fork();
            MyForkJoinTask subTask2 = new MyForkJoinTask(infosList.subList(mid + 1, infosList.size()));
            subTask1.fork();
            subTask2.fork();
            return subTask1.join() + subTask2.join();
        }
    }
}

class Infos {
    private String c1;
    private String c2;
    private String c3;

    public Infos() {
    }

    public Infos(String c1, String c2, String c3) {
        this.c1 = c1;
        this.c2 = c2;
        this.c3 = c3;
    }

    public String getC1() {
        return c1;
    }

    public void setC1(String c1) {
        this.c1 = c1;
    }

    public String getC2() {
        return c2;
    }

    public void setC2(String c2) {
        this.c2 = c2;
    }

    public String getC3() {
        return c3;
    }

    public void setC3(String c3) {
        this.c3 = c3;
    }
}