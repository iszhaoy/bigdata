package vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/15 18:12
 */
public class VerTxDemo {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx(
                new VertxOptions()
                        .setWorkerPoolSize(10)
                        .setEventLoopPoolSize(5)
        );

        JsonObject config = new JsonObject()
                .put("url", "jdbc:mysql://localhost:3306/cumin")
                .put("driver_class", "com.mysql.jdbc.Driver")
                .put("max_pool_size", 20)
                .put("user", "root")
                .put("password", "root");

        SQLClient sqlClient = JDBCClient.createShared(vertx, config);
        sqlClient.getConnection(connResult -> {
            System.out.println(connResult.failed());
            SQLConnection conn = connResult.result();
            conn.query("select * from cumin.goods", new Handler<AsyncResult<ResultSet>>() {
                @Override
                public void handle(AsyncResult<ResultSet> event) {
                    ResultSet result = event.result();
                    for (JsonObject row : result.getRows()) {
                        System.out.println(row.getInteger("goodsId"));
                        System.out.println(row.getString("goodsName"));
                    }
                }
            });
            conn.close();
        });

    }
}
