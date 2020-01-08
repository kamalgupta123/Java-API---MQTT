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
public class Detector {
    /**
     * Stores Connection to the database
     */
public Connection con = null;
     public Detector(Connection c){
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
    public List<Map<String, String>> getDetectorData(String type,String from,String to,String scn){
        List<Map<String, String>> DetectorData=null;   
        if(type.equals("raw")){
            DetectorData=getRawDetectorData(from,to,scn);
        }
        else{
            //System.out.print("kamal");
            DetectorData=getAggregatedDetectorData(from,to,scn);            
        }
        return DetectorData;
    }
    
    public List<Map<String, String>> getRawDetectorData(String from,String to,String scn){
        List<Map<String, String>> VBVData;
        List<Map<String, String>> ReplyData;
        List<Map<String, String>> RawDetectorData=new ArrayList();
        int site_id=0;
        String query="SELECT site_id,reply_timestamp,utcReplySDn FROM tis.utcReplyTable WHERE reply_timestamp BETWEEN ? AND ? AND site_id=? AND utcReplySDn IS NOT NULL";
        site_id=this.getSiteId(scn);
        ReplyData=this.RunSQL(query, from,to,site_id);
        String query1="SELECT Speed,Lane,ClassByAxle,TimeStamp,NumberOfAxle,SCN FROM `tis_detector_dynamic_vbv` WHERE SCN=? AND TimeStamp>=? AND TimeStamp<=?";
        VBVData=this.RunSQL(query1, scn,from,to);
        RawDetectorData.addAll(ReplyData);
        RawDetectorData.addAll(VBVData);
        return RawDetectorData;
    }
    
    public List<Map<String, String>> getAggregatedDetectorData(String from,String to,String scn){
        List<Map<String, String>> AggregatedDetectorData;
        String query="SELECT * FROM `utmc_detector_dynamic` WHERE SystemCodeNumber=? AND LastUpdated>=? AND LastUpdated<=?";
        AggregatedDetectorData=this.RunSQL(query, scn,from,to);
        return AggregatedDetectorData;
    }
    
     public int getSiteId(String SignalSCN){
        int site_id = 0;
        String query = "SELECT site_id FROM utmc_traffic_signal_static "
                + "WHERE SignalSCN=?";
        List<Map<String, String>> value = this.RunSQL(query, SignalSCN);
        try{
            site_id = Integer.parseInt(value.get(0).get("site_id"));
        }
        catch(IndexOutOfBoundsException ex){
            Logger.getLogger(TrafficData.class.getName()).log(Level.WARNING,
                    "No Site ID found for given Signal SCN");
        }
        return site_id;
    }

   
}