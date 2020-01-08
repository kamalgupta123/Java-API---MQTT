/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package serveriith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import serveriith.JSON.Message;

/**
 *
 * @author itspe
 */
public class ServerIITH implements MqttCallback {
    
    public static Connection con = null;   
    static TrafficData database = null;
    static ANPR database1 = null;
    static ATCC database2 = null;
    static Detector database3 = null;
    static SignalStage database4 = null;
    static SignalStatic database5 = null;
    Dblink dco;
    MqttClient client;
    String IPAddrress;
    String ip;
    String path;
    String IPFile = "IP.txt";
    public static void main(String[] args) {
        ServerIITH apihelper=new ServerIITH();
        apihelper.getConnection();
        database=new TrafficData(con);
        database1=new ANPR(con);
        database2=new ATCC(con);
        database3=new Detector(con);
        database4=new SignalStage(con);
        database5=new SignalStatic(con);
        apihelper.createSubscriber();
    }
    
    public void getConnection(){
        dco= new Dblink();
        con = dco.getConnection();
    }
    
    /**
     * creates a mqtt client so that in future it can subscribe or publish
     */
    public ServerIITH(){
         try {
             try {
                 File current = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
                 path = current.getParent();
             } catch (URISyntaxException ex) {
                 Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
             }
             File f = new File(path + "//" + IPFile);
             try {
               BufferedReader br = new BufferedReader(new FileReader(f));
                 ip = br.readLine();
             } catch (IOException ex) {
                 Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
             }
             client = new MqttClient("tcp://"+ip+":1883", MqttClient.generateClientId());
             client.connect();
         } catch (MqttException ex) {
             Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
         }
         
        
    }
    
    /**
     * publish the query result back to client in the form of json string 
     * @param k take json string of query result and publish it
     */
    public void createPublisher(String k){
        try{
            MqttMessage message = new MqttMessage();
            message.setPayload(k.getBytes());
            //message.setQos(0);
            client.publish("APIResult"+IPAddrress, message);
        }
        catch (MqttException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * creates a subscriber to listen to client to get query parameters from client to make query           later
     */
    public void createSubscriber(){
        try {
            client.setCallback(this);
            client.subscribe("APIHelper");
        } 
        catch (MqttException e) {
            e.printStackTrace();
        }
    } 
    @Override
    public void connectionLost(Throwable cause) {

    }
    
    /**
     * Takes a json string as input and parse it to Message class object
     * @param parser is json string
     * @param input is a message class object
     * @param res is the result of query put in message class object
     * @return message class object
     */
    public Message jsonParsing(JSON parser, Message input,List<Map<String, String>> res){
        Message newMessage = parser.new Message();
        newMessage.function=input.function;
        newMessage.parameters=input.parameters;
        newMessage.table=res;
        newMessage.ipAddress=input.ipAddress;
        newMessage.UUID=input.UUID;
        return newMessage;
    }
    
    
    /**
     * it gets json string of query parameters as input parse it and get function name which to call
     * to execute query and get result out of those parameters, calls function using switch statement
     * @param topic to which it subscribes for getting query parameter from client
     * @param message json string of query parameter
     * @throws Exception 
     */
    @Override
    public void messageArrived(String topic, MqttMessage message){
          String mess=message.toString();
          JSON jsonparser = new JSON();
          Message InputData = jsonparser.decode(mess);
          IPAddrress=InputData.ipAddress;
          switch(InputData.function){
            case "getVBVData":
                List<Map<String, String>> ret=null;
          try {
              ret = this.getVBVData(InputData.parameters.get(0), InputData.parameters.get(1),InputData.parameters.get(2));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage=jsonParsing(jsonparser,InputData,ret);
                String jsonString=jsonparser.encode(newMessage);
                createPublisher(jsonString);
                break;
            case "getRawDetectorData":
                List<Map<String, String>> ret2=null;
         try {
              ret2 = getRawDetectorData(InputData.parameters.get(2),InputData.parameters.get(1),InputData.parameters.get(0));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage2=jsonParsing(jsonparser,InputData,ret2);
                String jsonString2=jsonparser.encode(newMessage2);
                createPublisher(jsonString2);
                break;
            case "getLast30SecondData":
                List<Map<String, String>> ret3=null;
          try {
              ret3 = getLast30SecondData(InputData.parameters.get(0));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage3=jsonParsing(jsonparser,InputData,ret3);
                String jsonString3=jsonparser.encode(newMessage3);
                createPublisher(jsonString3);
                break;
            case "getRLVDData":
                List<Map<String, String>> ret4=null;
          try {
              ret4 = getRLVDData(InputData.parameters.get(0),InputData.parameters.get(1),Integer.parseInt(InputData.parameters.get(2)),Integer.parseInt(InputData.parameters.get(3)));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage4=jsonParsing(jsonparser,InputData,ret4);
                String jsonString4=jsonparser.encode(newMessage4);
                createPublisher(jsonString4);
                break;
            case "getTrafficDynamicData":
                List<Map<String, String>> ret5=null;
          try {
              ret5 = getTrafficDynamicData();
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage5=jsonParsing(jsonparser,InputData,ret5);
                String jsonString5=jsonparser.encode(newMessage5);
                createPublisher(jsonString5);
                break;
            case "getTrafficSignalFaultData":
                List<Map<String, String>> ret6=null;
          try {
              ret6 = getTrafficSignalFaultData();
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage6=jsonParsing(jsonparser,InputData,ret6);
                String jsonString6=jsonparser.encode(newMessage6);
                createPublisher(jsonString6);
                break;
            case "getAnprStaticData":
                List<Map<String, String>> ret7=null;
          try {
              System.out.println(this.getAnprStaticData(InputData.parameters.get(0),InputData.parameters.get(1),InputData.parameters.get(2)));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage7=jsonParsing(jsonparser,InputData,ret7);
                String jsonString7=jsonparser.encode(newMessage7);
                createPublisher(jsonString7);
                break;
            case "getAnprDynamicData":
                List<Map<String, String>> ret8=null;
          try {
              ret8 = this.getAnprDynamicData(InputData.parameters.get(0),InputData.parameters.get(1),InputData.parameters.get(2));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage8=jsonParsing(jsonparser,InputData,ret8);
                String jsonString8=jsonparser.encode(newMessage8);
                createPublisher(jsonString8);
                break;
          case "getAtccData":
                List<Map<String, String>> ret9=null;
          try {
              ret9 = this.getAtccData(InputData.parameters.get(0),InputData.parameters.get(1),InputData.parameters.get(2));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage9=jsonParsing(jsonparser,InputData,ret9);
                String jsonString9=jsonparser.encode(newMessage9);
                createPublisher(jsonString9);
                break; 
          case "getDetectorData":
                List<Map<String, String>> ret10=null;
          try {
              ret10 = this.getDetectorData(InputData.parameters.get(3),InputData.parameters.get(0),InputData.parameters.get(1),InputData.parameters.get(2));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage10=jsonParsing(jsonparser,InputData,ret10);
                String jsonString10=jsonparser.encode(newMessage10);
                createPublisher(jsonString10);
                break;
           case "setSignalStage":
               List<Map<String, String>> ret11=null;
          try {
              this.setSignalStage(InputData.parameters.get(0),InputData.parameters.get(1));
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage11=jsonParsing(jsonparser,InputData,ret11);
                String jsonString11=jsonparser.encode(newMessage11);
                createPublisher(jsonString11);
                break;
           case "getTrafficSignalConfigurations":
                List<Map<String, String>> ret12=null;
          try {
              ret12 = getTrafficSignalConfigurations();
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage12=jsonParsing(jsonparser,InputData,ret12);
                String jsonString12=jsonparser.encode(newMessage12);
                createPublisher(jsonString12);
                break;
            case "getTrafficSignalDefinitions":
                List<Map<String, String>> ret13=null;
          try {
              ret13 = getTrafficSignalDefinitions();
          } catch (Exception ex) {
              Logger.getLogger(ServerIITH.class.getName()).log(Level.SEVERE, null, ex);
          }
                Message newMessage13=jsonParsing(jsonparser,InputData,ret13);
                String jsonString13=jsonparser.encode(newMessage13);
                createPublisher(jsonString13);
                break;
                
           
        }
    }
    
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
      
    }
    
    /**
     * 
     * @return traffic signal dynamic data
     * @throws Exception 
     */
    public List<Map<String, String>> getTrafficDynamicData(){
        List<Map<String, String>> ret1= database.getTrafficDynamicData();
        return ret1;
    }
    
    /**
     * 
     * @return traffic signal fault data
     * @throws Exception 
     */
    public List<Map<String, String>> getTrafficSignalFaultData(){
        List<Map<String, String>> ret1= database.getTrafficSignalFaultData();
        return ret1;
    }
    
    /**
     * 
     * @param StartDate timestamp one
     * @param endDate timestamp two
     * @param limit start of record
     * @param offset end of record , how many records you want to fetch.
     * @return RLVD DATA between two timestamps
     * @throws Exception 
     */
    public List<Map<String, String>> getRLVDData(String StartDate,String endDate,int limit,int offset)     {
        List<Map<String, String>> ret1= database.getRLVDData(limit,offset,StartDate,endDate);
        return ret1;
    }
    
    /**
     * 
     * @param SignalSCN eg "J001"
     * @return returns last 30 sec data of traffic signal
     * @throws Exception 
     */
    public List<Map<String, String>> getLast30SecondData(String SignalSCN){
       List<Map<String, String>> ret3= database.getLast30SecondData(SignalSCN);
       return ret3;
        
    }
    
    /**
     * 
     * @param SignalSCN eg "J001"
     * @param toTimeStamp
     * @param fromTimeStamp
     * @return returns raw detector data between two timestamps
     * @throws Exception 
     */
    public List<Map<String, String>> getRawDetectorData(String SignalSCN,String toTimeStamp,String fromTimeStamp){
       List<Map<String, String>> ret2= database.getRawDetectorData(SignalSCN, Timestamp.valueOf(toTimeStamp),Timestamp.valueOf(fromTimeStamp));
       return ret2;        
    }
    
    
    /**
     * 
     * @param fromtime timestamp1
     * @param totime timestamp2
     * @return vehicle data between two timestamps.
     * @throws Exception 
     */
    public List<Map<String, String>> getVBVData(String SCN,String fromtime, String totime){
           List<Map<String, String>> ret= database.getVBVData(SCN, Timestamp.valueOf(fromtime),Timestamp.valueOf(totime));
           return ret;
           
   }
    
    public  String getAnprStaticData(String scn, String from, String to){
        String ret1= database1.getAnprStaticData(scn,from,to);
        return ret1;
    }
    

    public List<Map<String, String>> getAnprDynamicData(String scn, String from, String to){
        List<Map<String, String>> ret1= database1.getAnprDynamicData(scn,from,to);
        return ret1;
    }
 
    public List<Map<String, String>> getAtccData(String f,String t, String scn) throws Exception{
        List<Map<String, String>> ret1= database2.getAtccData(f,t,scn);
        return ret1;
    }   
    
     public List<Map<String, String>> getDetectorData(String type,String from,String to,String scn){
        List<Map<String, String>> ret1= database3.getDetectorData(type,from,to,scn);
        return ret1;
    }
     
    public void setSignalStage(String signalSCN,String nextStage){
        int nextstage=Integer.parseInt(nextStage);
        database4.setSignalStage(signalSCN,nextstage);
    }
    
    public List<Map<String, String>> getTrafficSignalConfigurations(){
        List<Map<String, String>> ret1= database5.getTrafficSignalConfigurations();
        return ret1;
    }
    
    /**
     * 
     * @return traffic signal fault data
     * @throws Exception 
     */
    public List<Map<String, String>> getTrafficSignalDefinitions(){
        List<Map<String, String>> ret1= database5.getTrafficSignalDefinitions();
        return ret1;
    } 
}
