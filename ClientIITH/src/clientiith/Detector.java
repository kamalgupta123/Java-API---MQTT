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


public class Detector {
        PublishSubscribe client1;
        public Detector(){
            client1 = PublishSubscribe.getInstance();
        }
        
        public List<Map<String, String>> getDetectorData(String type,String scn,Timestamp from,Timestamp to){
            String functionname="getDetectorData";
            List<Map<String, String>> DetectorData;
            JSON jsonparser=new JSON();
            Message mess = jsonparser.new Message();
            mess.function = functionname;
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(from.toString());
            mess.parameters.add(to.toString());
            mess.parameters.add(scn);
            mess.parameters.add(type);
            DetectorData = client1.createPublisher(mess);
            return DetectorData;
        }
        public void disconnect() throws MqttException{
            client1.client.disconnect();
        }
}