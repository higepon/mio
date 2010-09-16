import com.danga.MemCached.*;
import java.util.*;

public class JavaMemcachedClient
{
    public static void main(String[] args)
    {
        String[] serverlist = { "127.0.0.1:11211" };
        SockIOPool pool = SockIOPool.getInstance();
        pool.setServers(serverlist);
        pool.initialize();

        MemCachedClient mc = new MemCachedClient();
        mc.set("hello", "world");
        mc.set("intel", "cpu");
        mc.set("japan", "Tokyo");
        System.out.printf("hello => %s intel => %s japan => %s\n", mc.get("hello"), mc.get("intel"), mc.get("japan"));

        String[] keys = { "mio:range-search", "he", "j", "10", "asc" };
        Map<String, Object> ret = mc.getMulti(keys);

        for (Map.Entry<String, Object> e : ret.entrySet()) {
            System.out.println(e.getKey() + " : " + e.getValue());
        }
    }
}
