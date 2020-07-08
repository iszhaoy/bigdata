package exec;

import java.io.File;
import java.util.Objects;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 14:25
 */
public class FileToBlock  {
    public static void fileToBlock() {
        //从FileQueue中获取一个日志文件,如果没没有会产生阻塞
        try {
            File file = new File(Objects.requireNonNull(FileToBlock.class.getClassLoader().getResource("data.txt")).getPath());
            long length = file.length();
            //计算切块的数量
            long num = length % GlobalEnv.BLOCK_SIZE == 0 ?
                    length / GlobalEnv.BLOCK_SIZE :
                    length / GlobalEnv.BLOCK_SIZE + 1;
            //循环进行逻辑切块
            for (int i = 0; i < num; i++) {
                FileSplit split = new FileSplit();
                split.setPath(file.getPath());
                split.setStart(i * GlobalEnv.BLOCK_SIZE);
                //判断是否为最后一块（最后一块可能“不满”）
                if (i == num - 1) {
                    split.setLength(length - split.getStart());
                } else {
                    split.setLength(GlobalEnv.BLOCK_SIZE);
                }
                //将split添加的切块的队列中
//                GlobalEnv.getSplitQueue().add(split);
                GlobalEnv.getSplitList().add(split);
            }
            System.out.println("切片工完成");
        } catch (Exception e) {
            System.out.println("切片出现异常");
            e.printStackTrace();
        } finally {
            System.out.println("切片工具关闭");
        }
    }


}
