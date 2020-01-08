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
public class TrafficData {
    /**
     * Stores Connection to the database
     */
    public Connection con = null;
     public TrafficData(Connection c){
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
     * Gets Data related to RLVD between given timestamps.
     * @param PageNumber page number - starts from 1
     * @param recordsPerPage Number of records (row) per page
     * @param startDate yyyy-MM-dd HH:mm:ss
     * @param endDate yyyy-MM-dd HH:mm:ss
     * @return List of Map (row) 
     */
    public List<Map<String, String>> getRLVDData(int PageNumber,
            int recordsPerPage,String startDate,String endDate){
        int fromRecordNum = (recordsPerPage * PageNumber) - recordsPerPage;
        List<Map<String, String>> rlvd_data;
        String query="SELECT * FROM tis_rlvd_static AS t "
                + "INNER JOIN tis_raw_data_rlvd AS ta "
                + "ON t.SystemCodeNumber=ta.SystemCodeNumber "
                + "WHERE TimeStamp>='"+startDate+"'"
                + "AND TimeStamp<='"+endDate+"'"
                + " LIMIT ?, ?";
        rlvd_data = this.RunSQL(query, fromRecordNum,recordsPerPage);
        return rlvd_data;
    }
    
    /**
     * Gets Traffic Dynamic Data
     * @return List of Map objects(corresponding to each row) containing the data
     */
    public List<Map<String, String>> getTrafficDynamicData(){
        List<Map<String, String>> trafficSignalDynamic = null;
        String query3="SELECT 'http://www.livetrafficindia.com' as ImageLink,"
                + "'0.6' as CongestionPercent,cb.SystemCodeNumber,"
                + " cb.LastUpdated, cb.HistoricDate, cb.Colour, "
                + "cb.Reality, tis_signal_modes.mode_name as ControlStrategy,"
                + " cb.PlanNumber, cb.StageSequence, cb.PlanTimings,"
                + " utmc_traffic_signal_static.currentStageOrder "
                + "as CurrentStage, "
                + "group_concat(DISTINCT "
                + "concat(`utmc_signal_movements`.`from_link`,'-',"
                + "`utmc_signal_movements`.`to_link`)"
                + " separator ';') as VehicleMovement,"
                + " (case when utmc_traffic_signal_static.is_active = 0 "
                + "then 'Offline' else 'Online' end)"
                + " as Status FROM utmc_traffic_signal_dynamic cb "
                + "INNER JOIN utmc_traffic_signal_static "
                + "ON utmc_traffic_signal_static.SignalSCN ="
                + " cb.SystemCodeNumber "
                + "INNER JOIN `utmc_traffic_signal_stages` "
                + "ON `utmc_traffic_signal_stages`.`SignalSCN`="
                + " cb.SystemCodeNumber "
                + "AND utmc_traffic_signal_stages.StageOrder = "
                + "utmc_traffic_signal_static.currentStageOrder "
                + "INNER JOIN `utmc_signal_movements` "
                + "ON FIND_IN_SET(`utmc_signal_movements`.`id`, "
                + "REPLACE(TRIM("
                + "REPLACE(`utmc_traffic_signal_stages`.`VehicleMovements`,"
                + " ';', ' ')"
                + "), ' ', ',')) INNER JOIN `utmc_traffic_signal_static_links` "
                + "ON `utmc_traffic_signal_static_links`.`SignalSCN`="
                + "cb.SystemCodeNumber "
                + "AND (`utmc_signal_movements`.`from_link`="
                + "`utmc_traffic_signal_static_links`.`LinkOrder` "
                + "OR `utmc_signal_movements`.`to_link`="
                + "`utmc_traffic_signal_static_links`.`LinkOrder`) "
                + "INNER JOIN tis_signal_modes "
                + "ON tis_signal_modes.id = cb.ControlStrategy "
                + "WHERE cb.LastUpdated = (SELECT MAX(LastUpdated) "
                + "from `utmc_traffic_signal_dynamic` cb2 "
                + "WHERE cb2.`HistoricDate` IS NOT NULL "
                + "AND cb2.SystemCodeNumber = cb.SystemCodeNumber) "
                + "GROUP BY cb.SystemCodeNumber";
        trafficSignalDynamic = this.RunSQL(query3);
        return trafficSignalDynamic;
    }
    
    /**
     * Gets Traffic Signal Fault Data in List of HashMap
     * @return List of Map objects
     */
    public List<Map<String, String>> getTrafficSignalFaultData(){
        List<Map<String, String>> trafficSignalFault;
        List<Map<String, String>> SCN = this.getTrafficDynamicData();
        
        List<String> SCNs;
        SCNs = SCN.stream().map( s -> s.get("SystemCodeNumber"))
                .collect(Collectors.toList());
        
        String scn = String.join(",", SCNs);
        String scn_touse = scn.replace(",", "','");
        // Written in multiple lines to increase readability
        String query="select trafficSignalDynamic.SystemCodeNumber,"
                + "utmc_device_fault_typeid.FaultDescription as FaultTypeRef,"
                + "trafficSignalDynamic.CreationDate,"
                + "trafficSignalDynamic.LastUpdated,"
                + "trafficSignalDynamic.ClearedDate,"
                + "trafficSignalDynamic.Description,"
                + "utmc_device_subsystem_typeid.TypeDescription"
                + " as DataSourceTypeRef,"
                + "trafficSignalDynamic.FaultId,"
                + "trafficSignalDynamic.SupplierFaultNumber,"
                + "trafficSignalDynamic.SubSystemTypeID "
                + "as SubSystemTypeRef,"
                + "trafficSignalDynamic.EquipmentFault,"
                + "trafficSignalDynamic.CommunicationsFault,"
                + "trafficSignalDynamic.AckDate as AcknowledgmentDate,"
                + "trafficSignalDynamic.Ack_TypeID "
                + "as AcknowledgementStateTypeRef,"
                + "utmc_device_fault_typeid.FaultSeverity "
                + "FROM `utmc_device_fault` trafficSignalDynamic "
                + "LEFT JOIN `utmc_device_fault_typeid` "
                + "ON trafficSignalDynamic.FaultType = "
                + "utmc_device_fault_typeid.Fault_TypeID "
                + "INNER JOIN utmc_device_subsystem_typeid "
                + "ON utmc_device_subsystem_typeid.TypeID = "
                + "trafficSignalDynamic.DataSource_TypeID "
                + "WHERE trafficSignalDynamic.SystemCodeNumber IN ('" + scn_touse + "') "
                + "AND DATE(CreationDate) = CURDATE() "
                + "ORDER BY LastUpdated DESC";
        trafficSignalFault = this.RunSQL(query);
        return trafficSignalFault;
   }

    /**
     * Finds SiteID corresponding to SignalSCN 
     * @param SignalSCN eg. J001, J002 etc.
     * @return siteid for corresponding signal SCN
     */
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

    /**
     * Gets Data for last 30 seconds
     * @param SignalSCN eg. J001, J002 etc.
     * @return list of Map containing utcReplySDn and reply_timestamp 
     * conrresponding to given SignalSCN for last 30 seconds.
     */
    public List<Map<String, String>> getLast30SecondData(String SignalSCN){
        List<Map<String, String>> DetectorData;
        int site_id = this.getSiteId(SignalSCN);
        String query1 = "SELECT utcReplySDn,"
                + "reply_timestamp "
                + "FROM tis.utcReplyTable "
                + "WHERE reply_timestamp "
                + "BETWEEN timestamp(DATE_SUB(NOW(), INTERVAL 60 DAY)) "
                + "AND timestamp(NOW()) AND site_id=? "
                + "AND utcReplySDn IS NOT NULL LIMIT 3";
        DetectorData = this.RunSQL(query1, site_id);
        return DetectorData;
    }
    
    /**
     * Gets Vehicle Data
     * @param SCN eg 1.
     * @param fromTimeStamp eg 2019-09-19 16:56:04.
     * @param toTimeStamp
     * @return list of Map containing vehicle data between two timestamps
     */
    public List<Map<String, String>> getVBVData(String SCN,Timestamp fromTimeStamp,Timestamp toTimeStamp){
        List<Map<String, String>> VBVData;
        String query = "SELECT * FROM tis_detector_dynamic_vbv"
                + " WHERE TimeStamp >= ? and TimeStamp <= ? and SCN=?";
        VBVData = this.RunSQL(query,fromTimeStamp.toString(),toTimeStamp.toString(),SCN);
        return VBVData;
    }
    
    /**
     * Gets raw Detector Data
     * @param SignalSCN for example J001 TimeStamp for example '2019-09-19 16:56:04'.
     * @return list of Map containing raw detector data between two timestamps
     */
    public List<Map<String, String>> getRawDetectorData(String SignalSCN,Timestamp fromTimeStamp,Timestamp toTimeStamp){
        List<Map<String, String>> rawDetectorData;
        int site_id = this.getSiteId(SignalSCN);
        String query1 = "SELECT * FROM tis.utcReplyTable WHERE reply_timestamp >= ? AND reply_timestamp <= ? AND site_id = ?";
        rawDetectorData = this.RunSQL(query1,toTimeStamp.toString(),fromTimeStamp.toString(), site_id);
        return rawDetectorData;
    }
    
}