package iithapi;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author itspe
 */
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
/**
 *
 * @author itspe
 */
public class Client implements MqttCallback{
    MqttClient client;
    List<HashMap<String, String>> lis = new ArrayList<>();
    boolean status=false;

    public Client(){
        try {
            client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
            client.setCallback(this);
            client.connect();
            client.subscribe("APIResult");
        } catch (MqttException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    public  List<HashMap<String, String>> createPublisher(String query) throws Exception{
        status=false;
        try {
            MqttMessage message = new MqttMessage();
//            Timestamp d1 = Timestamp.valueOf("2019-09-12 16:56:04");
//            Timestamp d2 = Timestamp.valueOf("2019-09-19 16:56:04");
//            String query1 = "{";
//            query1 += "\"function\":\"runSQL\",";
//            query1 += "\"timestamp1\":" + "\"" + d1 + "\",";
//            query1 += "\"timestamp2\":" + "\"" + d2 + "\"";
//            query1 += "}";
            message.setPayload(query.getBytes());
            client.publish("APIHelper", message);
            while(!status){
                Thread.sleep(2000);
            }   
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return lis;
    }
    
    
    @Override
    public void connectionLost(Throwable cause) {
        // TODO Auto-generated method stub

    }
    /**
     * 
     * @param topic
     * @param message
     * @throws Exception 
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        lis = new ArrayList<HashMap<String, String>>();
        String k = message.toString();
        String z = k.substring(0, k.length() - 1);
        String t = z.substring(2, k.length() - 2);
        String[] rows = t.split("\\},\\{");
        String[] columns={};
        String[] label_value={};
        for(String row:rows){
            HashMap<String, String> Result = new HashMap<>();
            columns=row.split(",");
            for(String column:columns){
                label_value=column.split(":");
                Result.put(label_value[0].replace("\"", ""), label_value[1].replace("\"", ""));
            }
            lis.add(Result);
        }
//        String split1 = splits[0];
//        String split2 = splits[1];
//        String[] paramValue1 = split1.split(",");
//        String[] paramValue2 = split2.split(",");
//        String[] paramandvalue1 = {};
//        String[] paramandvalue2 = {};
//        HashMap<String, String> Result = new HashMap<>();
//        HashMap<String, String> Ret = new HashMap<>();
//        for (int i = 0; i < paramValue1.length; i++) {
//            paramandvalue1 = paramValue1[i].split(":");
//            for (int j = 0; j < paramandvalue1.length; j++) {
//                Result.put(paramandvalue1[0], paramandvalue1[1]);
//            }
//        }
//        for (int i = 0; i < paramValue2.length; i++) {
//            paramandvalue2 = paramValue2[i].split(":");
//            for (int j = 0; j < paramandvalue2.length; j++) {
//                Ret.put(paramandvalue2[0], paramandvalue2[1]);
//            }
//        }
//                for (HashMap<String, String> row : lis) {
//            System.out.println();
//            for (String e : row.keySet()) {
//                String key = e;
//                String value = row.get(e);
//                System.out.print(e + " : " + value + "\t");
//            }
//        }
        
        status=true;


    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // TODO Auto-generated method stub
    }    
}