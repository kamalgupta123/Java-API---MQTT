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


public class ANPR {
        PublishSubscribe client1;
        public ANPR(){
            client1 = PublishSubscribe.getInstance();
        }
        
        public List<Map<String, String>> getAnprDynamicData(String scn, Timestamp from, Timestamp to){
            String functionname="getAnprDynamicData";
            List<Map<String, String>> AnprDynamicData;
            JSON jsonparser=new JSON();
            Message mess = jsonparser.new Message();
            mess.function = functionname;
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(scn);
            mess.parameters.add(from.toString());
            mess.parameters.add(to.toString());
            AnprDynamicData = client1.createPublisher(mess);
            return AnprDynamicData;
        }
    
    /**
     * Gets raw Detector Data
     * @param SignalSCN for example J001 TimeStamp for example '2019-09-19 16:56:04'.
     * @return list of Map containing raw detector data between two timestamps
     */
        public List<Map<String, String>> getAnprStaticData(String scn, Timestamp from, Timestamp to){
            String functionname="getAnprStaticData";
            List<Map<String, String>> AnprStaticData;
            JSON jsonparser=new JSON();
            Message mess = jsonparser.new Message();
            mess.function = functionname;
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(scn);
            mess.parameters.add(from.toString());
            mess.parameters.add(to.toString());
            AnprStaticData = client1.createPublisher(mess);
            return AnprStaticData;
        }
        public void disconnect() throws MqttException{
            client1.client.disconnect();
        }
}