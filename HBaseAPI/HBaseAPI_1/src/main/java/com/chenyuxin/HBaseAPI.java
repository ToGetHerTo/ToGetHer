package com.chenyuxin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenshiliu
 * @create 2019-06-03 13:58
 */
@SuppressWarnings("all")
public class HBaseAPI {
    //获取Configuration对象
    public static Configuration conf;
    static {
        //使用HBaseConfiguration的单例方法实例化
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","zookeeper服务器节点地址");
            conf.set("hbase.zookeeper.property.clientPort","2181");
            Connection connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取配置信息和连接对象
     * @return
     * @throws Exception
     */
    public Connection getConnection() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    /**
     * 可以将下属创建admin的过程提出来
     * @param connection
     * @return
     * @throws IOException
     */
    public HBaseAdmin getAdmin(Connection connection) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin;
    }

    /**
     * 判断命名空间是否存在
     * @param connection
     * @return
     * @throws Exception
     */
    public boolean hasNamespace(String namespace ,Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        try {
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(namespace);
            return true;
        }catch ( Exception e){
            return false;
        }finally {
            admin.close();
        }
    }

    /**
     * 创建命名空间
     * @param connection
     * @throws Exception
     */
    public void createNamespace(Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        //不用new创建是因为其构造器均为私有的
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("chenyuxin").build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    /**
     * 判断表是否存在
     */
    public boolean isTableExist(String tableName, Connection connection) throws IOException {
        //在hbase中访问表需要先建立HBaseAdmin连接
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //或者可以这样：HBaseAdmin admin1= new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * 创建表(HBase1.0版本下)
     */
    public void createTable(String tableName,Connection connection,String... columnFamily) throws IOException {
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //判断表是否存在
        if(admin.tableExists(tableName)){
            System.out.println("表"+tableName+"已存在");
            System.exit(0);
        }else{
            //创建表属性对象，表明需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for(String cf:columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表"+tableName+"创建成功");
        }
    }

    /**
     * 创建表(HBase2.0版本下)
     */
    public void createTable2(Connection connection,String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否存在 其中tableName是否需要加valueOf取决于admin是Admin还是HBaseAdmin,但是新版本好像都需要加valueOf
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表"+tableName+"已存在");
            System.exit(0);
        }else{
// 由于没有引入HBase2.X的相关依赖所以会报错
        /*
            //创建表属性对象，表明需要转字节
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            //创建多个列族
            for(String cf:columnFamily){
                ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();//构建列族对象
                tableDescriptor.setColumnFamily(family);
            }
            //根据对表的配置，创建表
            admin.createTable(tableDescriptor.build());
            System.out.println("表"+tableName+"创建成功");
        */
        }
    }

    /**
     * 删除表
     */
    public void dropTable(String tableName,Connection connection) throws IOException {
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表"+tableName+"删除成功");
        }else{
            System.out.println("表"+tableName+"不存在");
        }
    }

    /**
     * 向表中插入数据
     */
    public void addRowData(String tableName,String rowkey,String columnFamily,
                           String column,String value ,Connection connection) throws IOException {
        //创建HTable对象
        HTable hTable = new HTable(conf,tableName);
        /*
        //新版本2.0
        Table table = connection.getTable(TableName.valueOf(tableName));
        */
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowkey));
        //向put对象中组装数据
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    /**
     * 删除多行数据
     */
    public void deleteMultiRow(String tableName,String... rows) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row:rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    /**
     * 获取所有数据
     */
    public void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell:cells){
                //得到rowkey
                System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族"+ Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列"+ Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值"+ Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取某一行数据
     */
    public void getRow(String tableName, String rowKey) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳版本
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()){
            System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列族"+ Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列"+ Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值"+ Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳" + cell.getTimestamp());
        }
    }

    /**
     * 获取某一行指定“列族：列”的数据
     */
    public void getRowQualifier(String tableName,String rowKey,String family,String qualifier) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列族"+ Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列"+ Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值"+ Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
}
