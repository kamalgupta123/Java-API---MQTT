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


public class SignalStage {
        PublishSubscribe client1;
        public SignalStage(){
            client1 =  PublishSubscribe.getInstance();
        }
        
        public void setSignalStage(String SignalSCN, String nextStage){
            String functionname="setSignalStage";
            JSON jsonparser=new JSON();
            Message mess = jsonparser.new Message();
            mess.function = functionname;
            mess.parameters=new ArrayList<String>();
            mess.parameters.add(SignalSCN);
            mess.parameters.add(nextStage);
            client1.createPublisher(mess);
        }
 
        public void disconnect() throws MqttException{
            client1.client.disconnect();
        }
}