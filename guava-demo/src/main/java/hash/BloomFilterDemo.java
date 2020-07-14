package hash;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/14 12:01
 */
public class BloomFilterDemo {
    public static void main(String[] args) {
        BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01);
        bloomFilter.put(1L);
        bloomFilter.put(2L);
        bloomFilter.put(3L);

        // 可能存在？
        boolean b = bloomFilter.mightContain(1L);
        System.out.println(b);

        // 一定不存在
        boolean b1 = bloomFilter.mightContain(4L);
        System.out.println(b1);

    }
}
