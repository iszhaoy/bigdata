import net.agkn.hll.HLL;
import org.roaringbitmap.RoaringBitmap;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/13 11:19
 */
public class TestJars {

    public static void main(String[] args) {
        // 测试RoaringBitmap
        RoaringBitmap r1 = new RoaringBitmap();

        r1.add(1);
        r1.add(2);
        r1.add(3);
        r1.add(1);
        // 判断是否包含
        System.out.println(r1.contains(4));
        System.out.println(r1.contains(1));
        System.out.println(r1.contains(2));

        // 遍历
        for (Integer i : r1) {
            System.out.println(i);
        }
        // 获取bitmap中的基数
        System.out.println(r1.getLongCardinality());

        System.out.println("``````````````````````````````");

        // 测试HyperLogLog
        HLL hll = new HLL(14, 6);
        hll.addRaw(1);
        hll.addRaw(2);
        hll.addRaw(3);
        hll.addRaw(4);
        hll.addRaw(1);
        hll.addRaw(3);

        // 获取hll中的
        long cardinality = hll.cardinality();
        System.out.println(cardinality);
    }
}
