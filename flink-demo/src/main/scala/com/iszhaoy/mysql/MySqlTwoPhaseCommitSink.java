package com.iszhaoy.mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/8 9:41
 */
@Slf4j
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<List<String>, Connection, Void> {

    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        log.info("123");

    }

    @Override
    protected void invoke(Connection connection, List<String> objectNode, Context context) throws Exception {
        log.info("start invoke...");
        String sql = "insert into t_test (value,total,insert_time) values (?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        log.info("objectNode {}",objectNode);
        for (String s : objectNode) {
            String[] split = s.split(",");
            ps.setString(1, split[0]);
            ps.setString(2, split[1]);

            Date date = new Date(System.currentTimeMillis());
            ps.setDate(3, date);

            log.info("要插入的数据:{}--{}", Arrays.toString(split), date);
            ps.addBatch();
        }
        //执行insert语句
        ps.executeBatch();
        //手动制造异常
        //int a = 1 / 0;
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");

        String url = "jdbc:mysql://localhost:3306/cumin";

        Connection connection = DBConnectUtil.getConnection(url, "root", "root");

        return connection;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");

        DBConnectUtil.commit(connection);
    }

    @Override
    protected void abort(Connection connection) {

        log.info("start abort rollback...");

        DBConnectUtil.rollback(connection);
    }
}
