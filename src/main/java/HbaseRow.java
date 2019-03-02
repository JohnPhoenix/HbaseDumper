import java.io.Serializable;

public class HbaseRow implements Serializable {
    private byte[] rowkey;
    private byte[] columnFamily;
    private byte[] qualifier;
    private byte[] value;


    public byte[] getRowkey() {
        return rowkey;
    }

    public void setRowkey(byte[] rowkey) {
        this.rowkey = rowkey;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(byte[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}




