package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Lin
 * @date 2019/5/8 - 12:17
 */
public class hbase_api {

    /**
     * 创建表,初始创建
     * @throws Exception
     */
    public static void createTable(String tab,String family) throws Exception{
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //根据配置信息，创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取管理对象
        Admin admin = conn.getAdmin();
        /**
         * 创建表;先判断表是否存在
         */
        TableName nst = TableName.valueOf(tab);
        boolean flg = admin.tableExists(nst);

        //创建表
        if(!flg){
            //准备表格描述器
            HTableDescriptor tableDescriptor = new HTableDescriptor(nst);
            //准备列描述器
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            //增加列族
            tableDescriptor.addFamily(columnDescriptor);
            //创建表
            admin.createTable(tableDescriptor);
            System.out.println("表格创建完成");
        }else {
            //先禁用表，才能删除
            admin.disableTable(nst);
            //删除表
            admin.deleteTable(nst);
            System.out.println("表格删除成功！");
        }
        //关闭资源
        conn.close();
        admin.close();
    }

    /**
     * 创建表时候选择命名空间创建，命名空间不存在则创建命名空间
     * @param tab
     * @param family
     * @param namespace
     * @throws Exception
     */
    public static void createTable(String tab,String family,String namespace) throws Exception{
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //根据配置信息，创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取管理对象
        Admin admin = conn.getAdmin();
        //判断namespace是否存在，不存在则抛出异常，然后创建
        try{
            admin.getNamespaceDescriptor(namespace);
        }catch(NamespaceNotFoundException e){
            //若发生特定的异常，即找不到命名空间，则创建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
        /**
         * 创建表;先判断表是否存在
         */
        TableName tableName = TableName.valueOf(namespace+":"+tab);
        boolean flg = admin.tableExists(tableName);

        //创建表
        if(!flg){

            //准备表格描述器
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            //准备列描述器
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            //增加列族
            tableDescriptor.addFamily(columnDescriptor);
            //创建表
            admin.createTable(tableDescriptor);
            System.out.println("表格创建完成");
        }else {
            //先禁用表，才能删除
            admin.disableTable(tableName);
            //删除表
            admin.deleteTable(tableName);
            System.out.println("表格删除成功！");
        }
        //关闭资源
        conn.close();
        admin.close();
    }

    /**
     * 创建表并预分区
     * @param tab
     * @param family
     * @param splitKeys
     * @throws Exception
     */
    public static void createTable(String tab,String family, byte[][] splitKeys) throws Exception{
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //根据配置信息，创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取管理对象
        Admin admin = conn.getAdmin();
        /**
         * 创建表;先判断表是否存在
         */
        TableName tableName = TableName.valueOf(tab);
        boolean flg = admin.tableExists(tableName);

        //创建表
        if(!flg){
            //准备表格描述器
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            //准备列描述器
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            //增加列族
            tableDescriptor.addFamily(columnDescriptor);
            //创建表
            admin.createTable(tableDescriptor,splitKeys);
            System.out.println("表格创建完成");
        }else {
            //先禁用表，才能删除
            admin.disableTable(tableName);
            //删除表
            admin.deleteTable(tableName);
            System.out.println("表格删除成功！");
        }
        //关闭资源
        conn.close();
        admin.close();
    }

    /**
     * 创建分区表并选择命名空间
     * @param tab
     * @param family
     * @param namespace
     * @param splitKeys
     * @throws Exception
     */
    public static void createTable(String tab,String family, String namespace, byte[][] splitKeys) throws Exception{
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //根据配置信息，创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取管理对象
        Admin admin = conn.getAdmin();
        /**
         * 创建表;先判断表是否存在
         */
        TableName tableName = TableName.valueOf(namespace+":"+tab);
        boolean flg = admin.tableExists(tableName);

        //创建表
        if(!flg){
            //准备表格描述器
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            //准备列描述器
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            //增加列族
            tableDescriptor.addFamily(columnDescriptor);
            //创建表
            admin.createTable(tableDescriptor,splitKeys);
            System.out.println("表格创建完成");
        }else {
            //先禁用表，才能删除
            admin.disableTable(tableName);
            //删除表
            admin.deleteTable(tableName);
            System.out.println("表格删除成功！");
        }
        //关闭资源
        conn.close();
        admin.close();
    }

    /**
     * 获取操作的表
     * @param tab
     * @return
     * @throws Exception
     */
    public static Table getTab(String tab) throws Exception{
        Table table = null;

        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //根据配置信息，创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取表对象
        TableName tableName = TableName.valueOf(tab);
        table = conn.getTable( tableName);

        return table;
    }


    /**
     * 遍历字段查询的数据
     * @param result
     */
    public static void getData(Result result) {
        //获取数据
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowkey = " + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("cf = " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("value = " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 按rowkey单一添加数据
     * @param tab
     * @param rk
     * @param fam
     * @param col
     * @param val
     * @throws Exception
     */
    public static void putDataForRowkey(String tab, String rk, String fam, String col, String val) throws Exception{
        Table table = getTab(tab);
        // 单一添加数据
        Put put = new Put(Bytes.toBytes(rk));

        byte[] family = Bytes.toBytes(fam);
        byte[] column = Bytes.toBytes(col);
        byte[] value = Bytes.toBytes(val);
       // byte[] fc = Bytes.toBytes(fam+":"+col);

        put.addColumn(family, column, new Date().getTime(), value);
        table.put(put);

        System.out.println("row:" + rk + " add success");
    }

    /**
     * 删除表
     * @param tablename
     * @throws Exception
     */
    public static void droptable(String tablename,String namespace) throws Exception {
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //根据配置信息，创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf(namespace+":"+tablename);
        boolean flg = admin.tableExists(tableName);
        if(flg){
            admin.disableTable(TableName.valueOf(namespace+":"+tablename));
            admin.deleteTable(TableName.valueOf(namespace+":"+tablename));
            System.out.println("Table " + tablename + " is droped!");
        }else{
            System.out.println("the table not exists!");
        }


    }

    /**
     * 删除多行数据
     * @param tab
     * @param rows
     * @throws Exception
     */
    public static void delDataForRows(String tab, String[] rows) throws Exception{
        Table table = getTab(tab);

        List<Delete> deleteList = new ArrayList<Delete>();

        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    /**
     * 按rowkey删除整行数据
     * @param tab
     * @param rk
     * @throws Exception
     */
    public static void delDataForRowkey(String tab, String rk) throws Exception{
        Table table = getTab(tab);

        Delete delete = new Delete(Bytes.toBytes(rk));
        table.delete(delete);

        System.out.println("row:" + rk + " is deleted");
        table.close();
    }

    /**
     * 根据rowkey删除列族下的某一个列
     * @param tab
     * @param rk
     * @param fam
     * @param col
     * @throws Exception
     */
    public static void delDataForRowket_column(String tab,String rk, String fam, String col) throws Exception{
        Table table = getTab(tab);

        Delete delete = new Delete(Bytes.toBytes(rk));
        delete.addColumn(Bytes.toBytes(fam), Bytes.toBytes(col));

        table.delete(delete);
        System.out.println("row:"+rk+"_famile:"+fam+"_column:"+col+"is delete" );
    }


    /**
     * 全量扫描
     * @param tab
     * @throws Exception
     */
    public static void scanALL(String tab) throws Exception{
        Table table = getTab(tab);

        //构建扫描对象
        Scan scan = new Scan();

        //获取扫描结果
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            getData(result);
        }
    }

    /**
     * 根据roekey查询整行数据
     * @param tab
     * @param rk
     * @throws Exception
     */
    public static void scanForRowkey(String tab,String rk) throws Exception{
        Table table = getTab(tab);

        Get get = new Get(Bytes.toBytes(rk));
        //get.setMaxVersions(3); // 设置一次性获取多少个版本的数据
        Result result = table.get(get);
        getData(result);
    }

    /**
     * 根据rowkey查询该行某一个列族
      * @param tab
     * @param rk
     * @param family
     * @throws Exception
     */
    public static void scanForRowkey(String tab,String rk, String family) throws Exception{
        Table table = getTab(tab);

        Get get = new Get(Bytes.toBytes(rk));
        get.addFamily(Bytes.toBytes(family));
        Result result = table.get(get);
        getData(result);
    }

    /**
     * 根据rowkey查询某一列族中的某一列(重载方法)
     * @param tab
     * @param rk
     * @param fam
     * @param col
     * @throws Exception
     */
    public static void scanForRowkey(String tab,String rk, String fam,String col) throws Exception{
        Table table = getTab(tab);

        Get get = new Get(Bytes.toBytes(rk));
        get.addColumn(Bytes.toBytes(fam),Bytes.toBytes(col));
        Result result = table.get(get);
        getData(result);
    }

    /**
     * start-stop范围扫描
     * @param tab
     * @param startRow
     * @param stopRow
     * @throws Exception
     */
    public static void scanRangeRowFilter(String tab,String startRow,String stopRow) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            getData(result);
        }
    }

    /**
     * start-stop范围扫描,扫描区间特定列族
     * @param tab
     * @param startRow
     * @param stopRow
     * @param family
     * @throws Exception
     */
    public static void scanRangeRowFilter(String tab,String startRow,String stopRow,String family) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            getData(result);
        }
    }

    /**
     * start-stop范围扫描，扫描区间特定列族的某一列
     * @param tab
     * @param startRow
     * @param stopRow
     * @param family
     * @param column
     * @throws Exception
     */
    public static void scanRangeRowFilter(String tab,String startRow,String stopRow,String family,String column) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            getData(result);
        }
    }

    /**
     * rowkeyFilter:扫描rk中包含某一字段的行
     * @param tab
     * @param r_k
     * @throws Exception
     */
    public static void scanContainRowFilter(String tab,String r_k) throws Exception {
        Table table = getTab(tab);

        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(r_k));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);

        }
    }

    /**
     *rowkeyFilter:扫描rk中包含某一字段的行,的某一列族
     * @param tab
     * @param r_k
     * @param family
     * @throws Exception
     */
    public static void scanContainRowFilter(String tab,String r_k,String family) throws Exception {
        Table table = getTab(tab);

        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(r_k));
        scan.setFilter(filter);
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);
        }
    }

    /**
     * rowkeyFilter:扫描rk中包含某一字段的行,的某一列族的某一列
     * @param tab
     * @param r_k
     * @param family
     * @param column
     * @throws Exception
     */
    public static void scanContainRowFilter(String tab,String r_k,String family,String column) throws Exception {
        Table table = getTab(tab);

        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(r_k));
        scan.setFilter(filter);
        scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);

        }
    }

    /**
     * 基于列族的过滤器FamilyFilter
     * @param tab
     * @param family
     * @throws Exception
     */
    public static void scanFamilyFilter(String tab,String family) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL , new BinaryComparator(Bytes.toBytes(family)));
        scan.setFilter(familyFilter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);
        }
    }

    /**
     * 返回所有行中以column[0]或者column[1]打头的列的数据
     * @param tab
     * @param column
     * @throws Exception
     */
    public static void scanCloumnFilter(String tab,String[] column) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        byte[][] bytes = new byte[][]{Bytes.toBytes(column[0]), Bytes.toBytes(column[1])};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(bytes);
        scan.setFilter(multipleColumnPrefixFilter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);
        }
    }

    /**
     * 列值过滤查询
     * @param tab
     * @param value
     * @throws Exception
     */
    public static void scanValueFilter(String tab,String value) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);
        }
    }

    /**
     * 单列值过滤器,会返回满足条件的整行
     * @param tab
     * @param family
     * @param column
     * @param value
     * @throws Exception
     */
    public static void scanSingleColumnValueFilter (String tab,String family,String column,
                                                    final CompareOperator op, String[][] value) throws Exception{
        Table table = getTab(tab);

        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes(family),
                Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(value[0][0]));
        filter.setFilterIfMissing(true); //如果不设置为 true，则那些不包含指定 column 的行也会返回
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            List<Cell> cells = result.listCells();
            getData(result);
        }
    }

    public static void main(String[] args) throws Exception {
        //scanSingleColumnValueFilter("student", "name", new String[]{"dalin", "weige"});
        scanALL("pie:test_spark");
        //命名空间，预分区，
        /*byte[][] splitKeys = {
                Bytes.toBytes("10"),
                Bytes.toBytes("20"),
                Bytes.toBytes("30")
        };
        createTable("student2","info", splitKeys);*/
        /*putDataForRowkey("student2","1","info","name","laodi");
        putDataForRowkey("student2","11","info","name","xiaolaodi");
        putDataForRowkey("student2","21","info","name","laodi");
        putDataForRowkey("student2","41","info","name","xiaolaodi");*/
       //createTable("student3","info","stu","stu:student3");
      //  droptable("student3");
      // droptable("stu:student" );
        /*byte[][] splitKeys = {
                Bytes.toBytes("10"),
                Bytes.toBytes("20"),
                Bytes.toBytes("30")
        };
        createTable("student2","info","stu",splitKeys);*/

    }
}
