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
public class SignalStage {
    String ForceBidNextStagePin;
    String site_id;
    String ug405_pin;
    String ug405_reply_pin;
    String fn_value;
    /**
     * Stores Connection to the database
     */
public Connection con = null;
     public SignalStage(Connection c){
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
    public void setSignalStage(String signalSCN, int nextStage){
        List<Map<String, String>> SignalStageData;
        String query = "SELECT `utmc_traffic_signal_static`.`ForceBidNextStagePin`,`utmc_traffic_signal_stages`.`ug405_reply_pin`,`utmc_traffic_signal_stages`.`ug405_pin`,`utmc_traffic_signal_static`.`site_id` FROM `utmc_traffic_signal_static` INNER JOIN `utmc_traffic_signal_stages` ON `utmc_traffic_signal_stages`.`SignalSCN`=`utmc_traffic_signal_static`.`SignalSCN` AND `utmc_traffic_signal_stages`.`StageOrder`=? WHERE `utmc_traffic_signal_static`.`SignalSCN`=?";
        SignalStageData = this.RunSQL(query,nextStage,signalSCN);
        for (Map<String, String> row :SignalStageData) {
            for (String e : row.keySet()) {
                String key = e;
                String value = row.get(e);
                if(key.equals("ForceBidNextStagePin")){
                    ForceBidNextStagePin=value;
                }
                else if(key.equals("site_id")){
                    site_id=value;
                }
                else if(key.equals("ug405_pin")){
                    ug405_pin=value;
                }
                else if(key.equals("ug405_reply_pin")){
                    ug405_reply_pin=value;
                }
            }
        }
        fn_value=Integer.toHexString(nextStage);
        String y="";
        for(int i=0;i<16-fn_value.length();i++){
            y+="0";
        }
        fn_value=y+fn_value;
        
        String query1="INSERT INTO `tis`.`utcControlTable`(`site_id`, `utcControlTimeStamp`, `utcControlFn`, utcControlTO, utcControlFF, utcControlLO) VALUES (?, 1, ?, 1, 0, 0)";
        PreparedStatement preparedQuery;
        try {
            preparedQuery = con.prepareStatement(query1);
            int siteid=Integer.parseInt(site_id);
            preparedQuery.setInt(1,siteid);
            preparedQuery.setString(2,fn_value);
            preparedQuery.executeUpdate(); 
        } catch (SQLException ex) {
            Logger.getLogger(TrafficData.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        String query2=query1.replace("utcControlTable","utcControlTable_dummy");
        try {
            preparedQuery = con.prepareStatement(query2);
            int siteid=Integer.parseInt(site_id);
            preparedQuery.setInt(1,siteid);
            preparedQuery.setString(2,fn_value);
            preparedQuery.executeUpdate(); 
        } catch (SQLException ex) {
            Logger.getLogger(TrafficData.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
}