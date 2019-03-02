import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

@Singleton
public class HBaseConnection {

        private static HConnection hConnection;

        private HBaseConnection() { }

        public static HConnection getConnection() {
            return hConnection;
        }

        public static HConnection createConnection(String hBaseZookeeperHostName,String port) throws IOException {
            try {
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", hBaseZookeeperHostName);
                conf.set("hbase.zookeeper.property.clientPort", port);
                hConnection = HConnectionManager.createConnection(conf);
                return hConnection;
            }catch (Exception ex) {
                ex.printStackTrace();
            }
            return null;
        }

        public static void closeConnection() throws IOException {
            if (hConnection != null) {
                hConnection.close();
            }
        }
    }
