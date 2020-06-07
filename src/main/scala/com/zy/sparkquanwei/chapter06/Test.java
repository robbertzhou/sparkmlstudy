package com.zy.sparkquanwei.chapter06;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {

    public static void main(String[] args) throws Exception{
        ExecutorService es = Executors.newFixedThreadPool(10);
        List<String> paths= getHbaseList("hdfs://master.zy.com:8020//hbase/data/news_info/d7fdd6b887b6fd04e4297e7190972ccd/f");
//        paths.add("hdfs://cdh94.fbi.com:8020/hbase/data/fbc/picture_file_info/b48634750d2269b180f8e0ab414d3501");
//                getHbaseList();
        System.out.println("任务总数：" + paths.size());
        for (int i=paths.size() - 1;i>=0;i--){
            System.out.println("任务：" + i);
            try{
                Test ts = new Test();
                ts.fp = paths.get(i);
                Thread th = new Thread(){
                    @Override
                    public void run() {
                        try {
                            ts.test();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                es.submit(th);
            }catch (Exception ex){

            }

        }
    }
    public static void write() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.client.keyvalue.maxsize","20971520");
        HTable htable = new HTable(conf, Bytes.toBytes("fbc:picture_file_info"));
        HTable bak = new HTable(conf, Bytes.toBytes("picture_file_info"));
        Scan scan = new Scan();
        long count = 0;
        int batch = 0;
        ResultScanner resultScanner = htable.getScanner(scan);
        try{
            for (Result result : resultScanner) {
                count++;
                List<Cell> cells= result.listCells();
                Put put = new Put(result.getRow());
                //关闭写前日志
                put.setWriteToWAL(false);
                for (Cell cell : cells){
                    byte[] col = cell.getQualifier();
                    byte[] family = cell.getFamily();
                    put.add(cell);
                }
                bak.put(put);
                if (count % 2000 == 0) {
                    bak.flushCommits();
                    System.out.println("提交批次:" + (batch++));
                }

            }
            bak.flushCommits();
        }catch (Exception ex){

        }

    }

    public static List<String> getHbaseList(String pp) throws Exception{
//        hdfs://cdh94.fbi.com:8020/hbase/data/fbc/picture_file_info
        List<String> paths = new ArrayList<String>();
        FileSystem hdfs = null;
        Configuration config = new Configuration();
        hdfs = FileSystem.get(new URI("hdfs://master.zy.com:8020/"),config, "hdfs");
        FileStatus[] files = hdfs.listStatus(new Path(pp));
        for (int i = 1; i < files.length; i++) {
            String fp = files[i].getPath().toUri().toString();
//            FileStatus[] fss = hdfs.listStatus(new Path(fp + "/f"));
            FileStatus[] fss = hdfs.listStatus(new Path(fp ));
            for (int j =0;j<fss.length;j++){
                paths.add(fss[j].getPath().toUri().toString());
            }
        }
        hdfs.close();
        return paths;
    }
    public String fp = "";

    public void test() throws Exception {
        try
        {
            String filePath = fp;
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.client.keyvalue.maxsize","209715200");
            CacheConfig cc = new CacheConfig(conf);
            HFile.Reader reader = HFile.createReader(FileSystem.get(conf),
                    new Path(filePath)
                    ,cc,conf);
            HFileScanner scanner = reader.getScanner(false,false);
            scanner.seekTo();
            HTable bak = new HTable(conf, Bytes.toBytes("fbc:news_info"));
            long count = 0;
            long batch = 0;
            while (scanner.next()){
                count++;
                Cell kk = scanner.getKeyValue();
                Put put = new Put(kk.getRow());
                //关闭写前日志
                put.setWriteToWAL(false);
                put.add(kk);
                bak.put(put);
                if (count % 2000 == 0) {
                    bak.flushCommits();
//                System.out.println("提交批次：" + (batch++));
                }
            }
            bak.flushCommits();
            System.out.println("完成的文件：" + filePath);
        }catch (Exception ex){
ex.printStackTrace();
        }

    }

}


