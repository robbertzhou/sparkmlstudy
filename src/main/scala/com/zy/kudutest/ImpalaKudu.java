package com.zy.kudutest;

import java.sql.*;

public class ImpalaKudu {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://master.zy.com:21050";

    public static void main(String[] args) throws Exception{
        Class.forName(JDBC_DRIVER);
        insert();
    }
    public static void query() throws Exception{
        Connection conn = DriverManager.getConnection(CONNECTION_URL);
        Statement st = conn.createStatement();
        ResultSet rs =st.executeQuery("select * from message limit 10");
        while (rs.next()){
            System.out.println(rs.getString("sender"));
        }
    }

    public static void insert() throws Exception{
        Connection conn = DriverManager.getConnection(CONNECTION_URL);
        PreparedStatement st = conn.prepareStatement("upsert into student values(?,?)");
        st.setLong(1,33);
        st.setString(2,"sewew");
        st.execute();
        conn.commit();

    }

}
