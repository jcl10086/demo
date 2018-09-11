package org.jcl.core;/**
 * Created by admin on 2018/9/10.
 */

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.jcl.util.DbUtils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jichenglu
 * @create 2018-09-10 17:32
 **/
public class HbaseSplit {

    public static void main(String[] args) throws IOException {

        List<String> vehicles=getVehicles();

        Admin admin=DbUtils.getHbaseConnection().getAdmin();
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("ccu_data_hl"));
        HColumnDescriptor hcdBase = new HColumnDescriptor("base_info".getBytes());
        HColumnDescriptor hcdCollect = new HColumnDescriptor("collect_info".getBytes());
        HColumnDescriptor hcdType = new HColumnDescriptor("type".getBytes());
        hcdBase.setCompressionType(Compression.Algorithm.SNAPPY);
        hcdCollect.setCompressionType(Compression.Algorithm.SNAPPY);
        hcdType.setCompressionType(Compression.Algorithm.SNAPPY);
        htd.addFamily(hcdBase);
        htd.addFamily(hcdCollect);
        htd.addFamily(hcdType);

        List<byte[]> list=vehicles.stream().map(x->(Bytes.toBytes(MD5Hash.getMD5AsHex(x.getBytes())))).collect(Collectors.toList());

        byte[][] splitKeys=new byte[list.size()][];
        list.forEach(x->{
            int i=list.indexOf(x);
            splitKeys[i]=x;
        });

        admin.createTable(htd,splitKeys);
        admin.close();

    }

    public static List<String> getVehicles(){
        List<String> vehicles=new ArrayList<>();
        try {
            java.sql.Connection conn=DbUtils.getJdbcConnection();
            String sql="select id from vehicle where name like 'CAMC0014%' or name like 'JMCH%'";
            PreparedStatement ps=conn.prepareStatement(sql);
            ResultSet rs=ps.executeQuery();
            while (rs.next()){
                String id=rs.getString(1);
                vehicles.add(id);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return vehicles;
    }
}
