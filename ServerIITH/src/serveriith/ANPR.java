/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package  serveriith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.Map;

/**
 * Class for acquiring traffic data from database.
 * @author itspe
 */
public class ANPR {
    /**
     * Stores Connection to the database
     */
     public Connection con = null;
     public ANPR(Connection c){
         con=c;
     }
    
    /**
     * Runs SQL Query and retruns result in the form of HashMap
     * @param query SQL query 
     * @param args Optional arguments in case query requires 
     * values of additional variables needs to be assigned
     * @return List of HashMap containing data recevied by running query.
     */
    private List<Map<String, String>> RunSQL(String query, Object ...args){
        List<Map<String, String>> ret = new ArrayList<>();
        try {
            PreparedStatement preparedQuery;
            preparedQuery = con.prepareStatement(query);
            int j = 1;
            for (Object s : args){
                if(s instanceof String){
                    preparedQuery.setString(j, (String)s);
                }
                else{
                    if (s instanceof Integer){
                        preparedQuery.setInt(j, (int)s);
                    }
                }
                j++;
            }
            ResultSet rs3=preparedQuery.executeQuery(); 
            ResultSetMetaData rsmd = rs3.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while(rs3.next()){
                int i=1;
                Map<String, String> Result = new HashMap<>();
                while(i<=columnsNumber){
                    Result.put(rsmd.getColumnName(i), rs3.getString(i));
                    i++;
                }
                ret.add(Result);
            }
            } catch (SQLException ex) {
            Logger.getLogger(TrafficData.class.getName()).log(Level.SEVERE,
                    ex.getMessage(), ex);
        }
            
        return ret;
    }
    
    /**
     * Gets Vehicle Data
     * @param SCN eg 1.
     * @param fromTimeStamp eg 2019-09-19 16:56:04.
     * @param toTimeStamp
     * @return list of Map containing vehicle data between two timestamps
     */
    public String getAnprStaticData(String scn,String from,String to){
        //List<Map<String, String>> AnprStaticData;
        //String query = "SELECT * FROM tis_anpr_static WHERE SystemCodeNumber= ? AND LastUpdated >=? AND LastUpdated <=?";
        //AnprStaticData = this.RunSQL(query,scn,from,to);
        return "CAM004";
    }
    
    public List<Map<String, String>> getAnprDynamicData(String scn,String from,String to){
        List<Map<String, String>> AnprDynamicData;
        String query = "SELECT * FROM tis_raw_data_rlvd WHERE SystemCodeNumber= ? AND TimeStamp >=? AND Timestamp <=?";
        AnprDynamicData = this.RunSQL(query,scn,from,to);
        return AnprDynamicData;
    }
   
}