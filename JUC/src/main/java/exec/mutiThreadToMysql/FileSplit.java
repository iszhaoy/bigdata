package exec.mutiThreadToMysql;

import java.util.StringJoiner;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/3 14:32
 */
public class FileSplit {
    private String path;
    private long start;
    private long length;

    public FileSplit() {
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FileSplit.class.getSimpleName() + "[", "]")
                .add("path='" + path + "'")
                .add("start=" + start)
                .add("length=" + length)
                .toString();
    }
}
