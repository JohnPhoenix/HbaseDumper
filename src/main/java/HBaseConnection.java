import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

@Singleton
public class HBaseConnection {

        private static Connection hConnection;

        private HBaseConnection() { }

        public static Connection getConnection() {
            return hConnection;
        }

        public static Connection createConnection(String hBaseZookeeperHostName,String port) {
            try {
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", hBaseZookeeperHostName);
                conf.set("hbase.zookeeper.property.clientPort", port);
                hConnection = ConnectionFactory.createConnection(conf);
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
