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


public class ATCC {
        PublishSubscribe client1;
        public ATCC(){
            client1 =  PublishSubscribe.getInstance();
        }
        
        public List<Map<String, String>> getAtccData(Timestamp fromtime,Timestamp totime,String SCN){
            String functionname="getAtccData";
            List<Map<String, String>> atccData;
            JSON jsonparser=new JSON();
            Message mess = jsonparser.new Message();
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(fromtime.toString());
            mess.parameters.add(totime.toString());
            mess.parameters.add(SCN);
            mess.function = functionname;
            atccData = client1.createPublisher(mess);
            return atccData;
        }
    
        public void disconnect(){
            try {
                client1.client.disconnect();
            } catch (MqttException ex) {
                Logger.getLogger(TrafficData.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
}