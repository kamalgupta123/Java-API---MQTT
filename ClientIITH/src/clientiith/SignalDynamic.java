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
import java.util.Map;
import java.util.stream.Collectors;


public class SignalDynamic {
        PublishSubscribe client1;
        public SignalDynamic(){
            client1 =  PublishSubscribe.getInstance();
        }
           
    /**
     * Gets Traffic Dynamic Data
     * @return List of Map objects(corresponding to each row) containing the data
     */
    public List<Map<String, String>> getTrafficDynamicData(){
        String functionname="getTrafficDynamicData";
        List<Map<String, String>> trafficSignalDynamic;
        JSON jsonparser=new JSON();
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
        JSON jsonparser=new JSON();
        Message mess = jsonparser.new Message();
        mess.function = functionname;
        trafficSignalFault=client1.createPublisher(mess);
        return trafficSignalFault;
    }
    
    public void disconnect() throws MqttException{
            client1.client.disconnect();
    }
}