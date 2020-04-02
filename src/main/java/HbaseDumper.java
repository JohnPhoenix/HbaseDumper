import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HbaseDumper {
    private static Connection hConnection;
    private static File dumperFile;
    private static FileOutputStream fileWriter;
    private static FileInputStream fileInputStream;
    private static ObjectInputStream objectInputStream;
    private static Table destTableInterface;


    public static void init(String[] argsParams) throws Exception {
        System.out.println("initializing..........");
        System.out.println(argsParams[0]+"\nConnecting to the Hbase on: "+argsParams[1]);
        hConnection = HBaseConnection.createConnection(argsParams[1].split(":")[0],argsParams[1].split(":")[1]);
        if(hConnection == null) {
            throw new Exception("Cant Connect to hbase");
        }
    }

    public static void main(String args[]) throws IOException {
        try {
            if(args == null)
                throw new Exception("Not Enough Parameters");

            String[] argParams = new String[args.length];
            System.arraycopy(args,0,argParams,0,args.length);
            init(argParams);
            if(argParams[0].equalsIgnoreCase("takedump")) {
                if(argParams.length != 3)
                    throw new Exception("Not Enough Parameters");
                takeDump(argParams);
            } else if (argParams[0].equalsIgnoreCase("load")) {
                if(argParams.length != 4)
                    throw new Exception("Not Enough Parameters");
                writeToHbase(argParams);
            } else {
                printWarning();
            }
        }catch (Exception ex) {
            printWarning();
            ex.printStackTrace();
        } finally {
            hConnection.close();
        }

    }


    public static void takeDump(String[] argParams) throws IOException {
        dumperFile = new File(argParams[2]+"_dump.txt");
        if(!dumperFile.exists()) {
            dumperFile.createNewFile();
        }
        fileWriter = new FileOutputStream(dumperFile);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileWriter);
        long limit = 0;
        Table sourceTableInterface = hConnection.getTable(TableName.valueOf(argParams[2].trim()));
        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCaching(100);
        ResultScanner scanner = sourceTableInterface.getScanner(scan);
        if (scanner != null) {
            for (Result result : scanner) {
                if (limit%1000 == 0){
                    System.out.println("Records dumped so far:" +limit);
                }
                limit++;
                if(result!= null) {
                    for (Cell kv : result.listCells()) {
                        HbaseRow hbaseRow = new HbaseRow();
                        hbaseRow.setRowkey(CellUtil.cloneRow(kv));
                        hbaseRow.setColumnFamily(CellUtil.cloneFamily(kv));
                        hbaseRow.setQualifier(CellUtil.cloneQualifier(kv));
                        hbaseRow.setValue(CellUtil.cloneValue(kv));
                        System.out.println(Bytes.toString(CellUtil.cloneRow(kv)));
                        objectOutputStream.writeObject(hbaseRow);
                    }
                }
            }
            System.out.println("Total Records dumped: " + limit);
        }
        fileWriter.close();
    }

    private static void writeToHbase(String[] argParams) throws IOException {
        dumperFile = new File(argParams[2]);
        List<Put> put = new ArrayList<>();
        HbaseRow hbaseRow;
        try {
            destTableInterface = hConnection.getTable(TableName.valueOf(argParams[3]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            objectInputStream = new ObjectInputStream(fileInputStream = new FileInputStream(dumperFile));
            while ((fileInputStream.available() > 0) && ((hbaseRow = (HbaseRow) objectInputStream.readObject()) != null)) {
                System.out.println(hbaseRow.toString());
                Put puttemp = new  Put(hbaseRow.getRowkey());
                if (hbaseRow.getRowkey() != null) {
                    puttemp.addColumn(hbaseRow.getColumnFamily(), hbaseRow.getQualifier(), hbaseRow.getValue());
                }else if(hbaseRow.getRowkey() == null) {
                    puttemp.addColumn(hbaseRow.getColumnFamily(), hbaseRow.getQualifier(), "".getBytes());
                }else {
                    System.out.println("invalid data ");
                }
                put.add(puttemp);
                if(put.size() == 100) {
                    destTableInterface.put(put);
                    put.clear();
                }
            }
            if(put.size() != 0) {
                destTableInterface.put(put);
                put.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(fileInputStream != null) {
                fileInputStream.close();
            }
            if(objectInputStream != null) {
                objectInputStream.close();
            }
        }
    }

    private static void printWarning() {
        System.out.println("Not Enough Parameterts\n" +
                "Operations that can be performed \n.1.takedump \n2.load\n" +
                "For taking Dump \n" +
                "java -jar HbaseDumper-1.0-SNAPSHOT-all.jar takedump <HbaseZookeeperHostName>:<port> <src_table_name> \n" +
                "For Loading the dumped file to hbase....\n" +
                "java -jar HbaseDumper-1.0-SNAPSHOT-all.jar load <HbaseZookeeperHostName>:<port> <Dump_file_Name> <dest_table_name> ");
    }

}
