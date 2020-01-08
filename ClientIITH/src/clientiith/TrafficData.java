/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clientiith;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.MqttException;
import clientiith.JSON.Message;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class TrafficData {
        PublishSubscribe client1;
        JSON jsonparser=null;
        public TrafficData(){
            jsonparser=new JSON();
            client1 = PublishSubscribe.getInstance();
        }
        
        /**
         * 
         * @param PageNumber the page of the particular record you want to fetch
         * @param recordsPerPage how many records you want to get,how may records there would be in            one page.
         * @param startDate the starting date above which you want to get records 
         * @param endDate the ending date below which the records should be.
         * @return returns RLVD Data between two timestamps in the form of list of hashmap
         * @throws Exception 
         */
    
        public List<Map<String, String>> getRLVDData(int PageNumber,
            int recordsPerPage,Timestamp startDate,Timestamp endDate){
            String functionname="getRLVDData";
            List<Map<String, String>> rlvd_data;
            Message mess = jsonparser.new Message();
            mess.function = functionname;
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(startDate.toString());
            mess.parameters.add(endDate.toString());
            mess.parameters.add(String.valueOf(PageNumber));
            mess.parameters.add(String.valueOf(recordsPerPage));
            rlvd_data=client1.createPublisher(mess);
            return rlvd_data;
    }
    
    /**
     * Gets Traffic Dynamic Data
     * @return List of Map objects(corresponding to each row) containing the data
     */
    public List<Map<String, String>> getTrafficDynamicData(){        
        String functionname="getTrafficDynamicData";
        List<Map<String, String>> trafficSignalDynamic;
        Message mess = jsonparser.new Message();
        mess.function = functionname;
        trafficSignalDynamic=client1.createPublisher(mess);
        return trafficSignalDynamic;
    }
    
    /**
     * Gets Traffic Signal Fault Data in List of HashMap
     * @return List of Map objects
     */
    public List<Map<String, String>> getTrafficSignalFaultData(){
        String functionname="getTrafficSignalFaultData";
        List<Map<String, String>> trafficSignalFault;
        Message mess = jsonparser.new Message();
        mess.function = functionname;
        trafficSignalFault=client1.createPublisher(mess);
        return trafficSignalFault;
   }

    
    /**
     * Gets Data for last 30 seconds
     * @param SignalSCN eg. J001, J002 etc.
     * @return list of Map containing utcReplySDn and reply_timestamp 
     * conrresponding to given SignalSCN for last 30 seconds.
     */
    public List<Map<String, String>> getLast30SecondData(String SignalSCN){
        String functionname="getLast30SecondData";
        List<Map<String, String>> DetectorData;
        Message mess = jsonparser.new Message();
        mess.function = functionname;
        mess.parameters=new ArrayList<String>();
        mess.parameters.add(SignalSCN);
        DetectorData = client1.createPublisher(mess);
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
        String functionname="getVBVData";
        List<Map<String, String>> VBVData;
        Message mess = jsonparser.new Message();
        mess.function = functionname;
        mess.parameters=new ArrayList<String>();
        mess.parameters.add(SCN);
        mess.parameters.add(fromTimeStamp.toString());
        mess.parameters.add(toTimeStamp.toString());
        VBVData = client1.createPublisher(mess);        
        return VBVData;
    }
    
    /**
     * Gets raw Detector Data
     * @param SignalSCN for example J001 TimeStamp for example '2019-09-19 16:56:04'.
     * @return list of Map containing raw detector data between two timestamps
     */
        public List<Map<String, String>> getRawDetectorData(String SignalSCN,Timestamp fromTimeStamp,Timestamp toTimeStamp){
            String functionname="getRawDetectorData";
            List<Map<String, String>> rawDetectorData;
            Message mess = jsonparser.new Message();
            mess.function = functionname;
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(fromTimeStamp.toString());
            mess.parameters.add(toTimeStamp.toString());
            mess.parameters.add(SignalSCN);
            rawDetectorData = client1.createPublisher(mess);
            return rawDetectorData;
        }
        public void disconnect() throws MqttException{
            client1.client.disconnect();
        }
}
