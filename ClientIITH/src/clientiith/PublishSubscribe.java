package clientiith;

import clientiith.JSON.Message;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


public class PublishSubscribe implements MqttCallback{
    MqttClient client;
    String IPAddrress;
    InetAddress IP;
    String IPFile = "IP.txt";
    String ip;
    String path;
    List<Map<String, String>> lis = new ArrayList<>();
    boolean status=false;
    String UUID1=null;
    String UUID2=null;
    String IPAddress=null;
    String id = null;
    JSON parse = null;
    private static PublishSubscribe publishsubscribe=null;
    /**
 * sets up a client that subscribes to a channel to listen to all the messages subscribed to that       channel, so it will listen to result of query sent by server.
     */
    public static PublishSubscribe getInstance(){
        if(publishsubscribe==null){
            publishsubscribe = new PublishSubscribe();
        }
        return publishsubscribe;
    }
    
    private PublishSubscribe(){
        getIPAddress();
        getUUID();
        parse = new JSON();
        try {
            try {
                File current = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
                 path = current.getParent();
             } catch (URISyntaxException ex) {
                 Logger.getLogger(PublishSubscribe.class.getName()).log(Level.SEVERE, null, ex);
             }
             File f = new File(path + "//" + IPFile);
             try {
               BufferedReader br = new BufferedReader(new FileReader(f));
                 ip = br.readLine();
             } catch (IOException ex) {
                 Logger.getLogger(PublishSubscribe.class.getName()).log(Level.SEVERE, null, ex);
             }
            client = new MqttClient("tcp://"+ip+":1883", MqttClient.generateClientId());
            client.setCallback(this);
            client.connect();
            try {
                Enumeration<NetworkInterface> nics = NetworkInterface
                    .getNetworkInterfaces();
                while (nics.hasMoreElements()) {
                    NetworkInterface nic = nics.nextElement();
                    Enumeration<InetAddress> addrs = nic.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        InetAddress addr = addrs.nextElement();
                        if(addr.isSiteLocalAddress()){
                            IPAddrress = addr.getHostAddress().toString();
                        }
                    }
                }
            }
            catch (SocketException e) {
               e.printStackTrace();
            }
            client.subscribe("APIResult"+IPAddrress);
        } catch (MqttException ex) {
            Logger.getLogger(PublishSubscribe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
     public void getIPAddress(){
            try {
                   Enumeration<NetworkInterface> nics = NetworkInterface
                       .getNetworkInterfaces();
                   while (nics.hasMoreElements()) {
                       NetworkInterface nic = nics.nextElement();
                       Enumeration<InetAddress> addrs = nic.getInetAddresses();
                       while (addrs.hasMoreElements()) {
                           InetAddress addr = addrs.nextElement();
                           if(addr.isSiteLocalAddress()){
                               IPAddress = addr.getHostAddress().toString();
                           }
                       }
                   }
               }
           catch (SocketException e) {
                  e.printStackTrace();
           }    
        }
        
        public void getUUID(){
            UUID uniqueid= UUID.randomUUID(); 
            id=uniqueid.toString();
        }
    
    /**
     * Takes a json string of query parameters as input to later put those parameters in the query.         publishes those parameters on a channel so server listens to that channel get the query parameters     and put those parameters into query. 
     * @param json string of query parameters.
     * @return the result of the query fetched from database in a list of hashmap
     * @throws Exception 
     */
    public  List<Map<String, String>> createPublisher(Message query){
        status=false;
        try {
            query.UUID=id;
            query.ipAddress=IPAddress;
            String jsonString = parse.encode(query);
            MqttMessage message = new MqttMessage();
            message.setPayload(jsonString.getBytes());
            //message.setQos(0);
            System.out.print(status);
            client.publish("APIHelper", message);
            //client.publish("APIHelper", message);
            while(!status){
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(PublishSubscribe.class.getName()).log(Level.SEVERE, null, ex);
                }
            }   
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return lis;
    }
    
    
    @Override
    public void connectionLost(Throwable cause) {

    }
    
    /**
     * it listens to the result of query passed by server to it and store that result into hashmap
     * so createPublisher() can return it
     * @param topic on which it subscribes and server publishes so it gets query result from server
     * @param message the result of query is contained in message
     * @throws Exception 
     */
    
    @Override
    public void messageArrived(String topic, MqttMessage message){ 
          lis = new ArrayList<Map<String, String>>();
          String mess = message.toString();
          Message jsonmessage = parse.new Message();
          jsonmessage=parse.decode(mess);
          UUID2=jsonmessage.UUID;
          if(id.equals(UUID2)){
            lis=jsonmessage.table;
            status=true;
          }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        
    }    
}
