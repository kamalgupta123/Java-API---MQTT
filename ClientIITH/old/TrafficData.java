/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iithapi;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 *
 * @author itspe
 */
public class TrafficData {
        Client client1;
        public TrafficData(){
            client1 = new Client();
        }
    
//        public List<HashMap<String, String>> getRLVDData(int PageNumber,
//            int recordsPerPage,String startDate,String endDate){
//        int fromRecordNum = (recordsPerPage * PageNumber) - recordsPerPage;
//        List<HashMap<String, String>> rlvd_data;
////        String query="SELECT * FROM tis_rlvd_static AS t "
////                + "INNER JOIN tis_raw_data_rlvd AS ta "
////                + "ON t.SystemCodeNumber=ta.SystemCodeNumber "
////                + "WHERE TimeStamp>='"+startDate+"'"
////                + "AND TimeStamp<='"+endDate+"'"
////                + " LIMIT ?, ?";
//        String query1 = "{";
//            query1 += "\"function\":\"runSQL\",";
//            query1 += "\"fromRecordNum\":" + "\"" + startDate + "\",";
//            query1 += "\"fromRecordNum\":" + "\"" + endDate + "\",";
//            query1 += "\"fromRecordNum\":" + "\"" + fromRecordNum + "\",";
//            query1 += "\"recordsPerPage\":" + "\"" + recordsPerPage + "\"";
//            query1 += "}";
//        return rlvd_data;
//    }
    
//    /**
//     * Gets Traffic Dynamic Data
//     * @return List of Map objects(corresponding to each row) containing the data
//     */
//    public List<HashMap<String, String>> getTrafficDynamicData(){
//        List<HashMap<String, String>> trafficSignalDynamic = null;
//        String query3="SELECT 'http://www.livetrafficindia.com' as ImageLink,"
//                + "'0.6' as CongestionPercent,cb.SystemCodeNumber,"
//                + " cb.LastUpdated, cb.HistoricDate, cb.Colour, "
//                + "cb.Reality, tis_signal_modes.mode_name as ControlStrategy,"
//                + " cb.PlanNumber, cb.StageSequence, cb.PlanTimings,"
//                + " utmc_traffic_signal_static.currentStageOrder "
//                + "as CurrentStage, "
//                + "group_concat(DISTINCT "
//                + "concat(`utmc_signal_movements`.`from_link`,'-',"
//                + "`utmc_signal_movements`.`to_link`)"
//                + " separator ';') as VehicleMovement,"
//                + " (case when utmc_traffic_signal_static.is_active = 0 "
//                + "then 'Offline' else 'Online' end)"
//                + " as Status FROM utmc_traffic_signal_dynamic cb "
//                + "INNER JOIN utmc_traffic_signal_static "
//                + "ON utmc_traffic_signal_static.SignalSCN ="
//                + " cb.SystemCodeNumber "
//                + "INNER JOIN `utmc_traffic_signal_stages` "
//                + "ON `utmc_traffic_signal_stages`.`SignalSCN`="
//                + " cb.SystemCodeNumber "
//                + "AND utmc_traffic_signal_stages.StageOrder = "
//                + "utmc_traffic_signal_static.currentStageOrder "
//                + "INNER JOIN `utmc_signal_movements` "
//                + "ON FIND_IN_SET(`utmc_signal_movements`.`id`, "
//                + "REPLACE(TRIM("
//                + "REPLACE(`utmc_traffic_signal_stages`.`VehicleMovements`,"
//                + " ';', ' ')"
//                + "), ' ', ',')) INNER JOIN `utmc_traffic_signal_static_links` "
//                + "ON `utmc_traffic_signal_static_links`.`SignalSCN`="
//                + "cb.SystemCodeNumber "
//                + "AND (`utmc_signal_movements`.`from_link`="
//                + "`utmc_traffic_signal_static_links`.`LinkOrder` "
//                + "OR `utmc_signal_movements`.`to_link`="
//                + "`utmc_traffic_signal_static_links`.`LinkOrder`) "
//                + "INNER JOIN tis_signal_modes "
//                + "ON tis_signal_modes.id = cb.ControlStrategy "
//                + "WHERE cb.LastUpdated = (SELECT MAX(LastUpdated) "
//                + "from `utmc_traffic_signal_dynamic` cb2 "
//                + "WHERE cb2.`HistoricDate` IS NOT NULL "
//                + "AND cb2.SystemCodeNumber = cb.SystemCodeNumber) "
//                + "GROUP BY cb.SystemCodeNumber";
//        return trafficSignalDynamic;
//    }
//    
//    /**
//     * Gets Traffic Signal Fault Data in List of HashMap
//     * @return List of Map objects
//     */
//    public List<HashMap<String, String>> getTrafficSignalFaultData(){
//        List<HashMap<String, String>> trafficSignalFault;
//        List<HashMap<String, String>> SCN = this.getTrafficDynamicData();
//        
//        List<String> SCNs;
//        SCNs = SCN.stream().map( s -> s.get("SystemCodeNumber"))
//                .collect(Collectors.toList());
//        
//        String scn = String.join(",", SCNs);
//        String scn_touse = scn.replace(",", "','");
//        // Written in multiple lines to increase readability
//        String query="select trafficSignalDynamic.SystemCodeNumber,"
//                + "utmc_device_fault_typeid.FaultDescription as FaultTypeRef,"
//                + "trafficSignalDynamic.CreationDate,"
//                + "trafficSignalDynamic.LastUpdated,"
//                + "trafficSignalDynamic.ClearedDate,"
//                + "trafficSignalDynamic.Description,"
//                + "utmc_device_subsystem_typeid.TypeDescription"
//                + " as DataSourceTypeRef,"
//                + "trafficSignalDynamic.FaultId,"
//                + "trafficSignalDynamic.SupplierFaultNumber,"
//                + "trafficSignalDynamic.SubSystemTypeID "
//                + "as SubSystemTypeRef,"
//                + "trafficSignalDynamic.EquipmentFault,"
//                + "trafficSignalDynamic.CommunicationsFault,"
//                + "trafficSignalDynamic.AckDate as AcknowledgmentDate,"
//                + "trafficSignalDynamic.Ack_TypeID "
//                + "as AcknowledgementStateTypeRef,"
//                + "utmc_device_fault_typeid.FaultSeverity "
//                + "FROM `utmc_device_fault` trafficSignalDynamic "
//                + "LEFT JOIN `utmc_device_fault_typeid` "
//                + "ON trafficSignalDynamic.FaultType = "
//                + "utmc_device_fault_typeid.Fault_TypeID "
//                + "INNER JOIN utmc_device_subsystem_typeid "
//                + "ON utmc_device_subsystem_typeid.TypeID = "
//                + "trafficSignalDynamic.DataSource_TypeID "
//                + "WHERE trafficSignalDynamic.SystemCodeNumber IN (?) "
//                + "AND DATE(CreationDate) = CURDATE() "
//                + "ORDER BY LastUpdated DESC";
//        return trafficSignalFault;
//   }
//
//    /**
//     * Finds SiteID corresponding to SignalSCN 
//     * @param SignalSCN eg. J001, J002 etc.
//     * @return siteid for corresponding signal SCN
//     */
    public int getSiteId(String SignalSCN) throws Exception{
        int site_id = 0;
//        String query = "SELECT site_id FROM utmc_traffic_signal_static "
//                + "WHERE SignalSCN=?";
        String query1 = "{";
        query1 += "\"function\":\"getSiteId\",";
        query1 += "\"SignalSCN\":" + "\"" + SignalSCN + "\"";
        query1 += "}";
        List<HashMap<String, String>> value;
        value=client1.createPublisher(query1);
        try{
            HashMap<String,String> temp = value.get(0);
            //System.out.println(temp.get(temp.keySet().toArray()[0]).trim().);
            site_id = Integer.parseInt(value.get(0).get("site_id").trim());
        }
        catch(IndexOutOfBoundsException ex){
            Logger.getLogger(TrafficData.class.getName()).log(Level.WARNING,
                    "No Site ID found for given Signal SCN");
        }
        return site_id;
    }
//
//    /**
//     * Gets Data for last 30 seconds
//     * @param SignalSCN eg. J001, J002 etc.
//     * @return list of Map containing utcReplySDn and reply_timestamp 
//     * conrresponding to given SignalSCN for last 30 seconds.
//     */
    public List<HashMap<String, String>> getLast30SecondData(String SignalSCN) throws Exception{
        List<HashMap<String, String>> DetectorData;
        int site_id = this.getSiteId(SignalSCN);
        String query1 = "{";
        query1 += "\"function\":\"getLast30SecondData\",";
        query1 += "\"site_id\":" + "\"" + site_id + "\"";
        query1 += "}";
//        String query = "SELECT utcReplySDn,"
//                + "reply_timestamp "
//                + "FROM tis.utcReplyTable "
//                + "WHERE reply_timestamp "
//                + "BETWEEN timestamp(DATE_SUB(NOW(), INTERVAL 30 SECOND)) "
//                + "AND timestamp(NOW()) AND site_id=? "
//                + "AND utcReplySDn IS NOT NULL";
        DetectorData = client1.createPublisher(query1);
        return DetectorData;
    }
//    
//    /**
//     * Gets Vehicle Data
//     * @param SCN eg 1.
//     * @param fromTimeStamp eg 2019-09-19 16:56:04.
//     * @param toTimeStamp
//     * @return list of Map containing vehicle data between two timestamps
//     */
    public List<HashMap<String, String>> getVBVData(Timestamp fromTimeStamp,Timestamp toTimeStamp) throws Exception{
        List<HashMap<String, String>> VBVData;
        String query1 = "{";
        query1 += "\"function\":\"runSQL\",";
        query1 += "\"timestamp1\":" + "\"" + fromTimeStamp + "\",";
        query1 += "\"timestamp2\":" + "\"" + toTimeStamp + "\"";
        query1 += "}";
        VBVData = client1.createPublisher(query1);
//         for (HashMap<String, String> row : VBVData) {
//                System.out.println();
//                for (String e : row.keySet()) {
//                    String key = e;
//                    String value = row.get(e);
//                    System.out.print(e + " : " + value + "\t");
//                }
//            }
        
        return VBVData;
    }
    
//    /**
//     * Gets raw Detector Data
//     * @param SignalSCN for example J001 TimeStamp for example '2019-09-19 16:56:04'.
//     * @return list of Map containing raw detector data between two timestamps
//     */
        public List<HashMap<String, String>> getRawDetectorData(String SignalSCN,Timestamp fromTimeStamp,Timestamp toTimeStamp) throws Exception{
            List<HashMap<String, String>> rawDetectorData;
            int site_id = this.getSiteId(SignalSCN);
            String query1 = "{";
            query1 += "\"function\":\"getRawDetectorData\",";
            query1 += "\"timestamp1\":" + "\"" + fromTimeStamp + "\",";
            query1 += "\"timestamp2\":" + "\"" + toTimeStamp + "\",";
            query1 += "\"site_id\":" + "\"" + site_id + "\"";
            query1 += "}";
            rawDetectorData = client1.createPublisher(query1);
//            String query = "SELECT * FROM tis.utcReplyTable WHERE reply_timestamp >= ? AND reply_                 timestamp <= ? AND site_id = ?";
            return rawDetectorData;
        }
        public void disconnect() throws MqttException{
            client1.client.disconnect();
        }
}
