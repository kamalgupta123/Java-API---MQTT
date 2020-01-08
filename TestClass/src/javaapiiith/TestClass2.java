/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapiiith;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import clientiith.SignalStatic;
import clientiith.TrafficData;
import java.time.LocalDateTime;
//import atccclient.TrafficData;
import java.util.ArrayList;
import java.util.Map;
import org.json.simple.JSONObject; 

/**
 *
 * @author itspe
 */
public class TestClass2{
    
    public static void main(String args[]) throws Exception{
        SignalStatic k = new SignalStatic();
        TrafficData k1 = new TrafficData();
        Timestamp d5= Timestamp.valueOf("2018-03-02 13:47:01");
        Timestamp d6= Timestamp.valueOf("2018-03-02 13:47:01");
        Timestamp d1= Timestamp.valueOf("2019-09-12 16:56:04");
        Timestamp d2=Timestamp.valueOf("2019-09-19 16:56:04");
        Timestamp d3=Timestamp.valueOf("2019-02-27 05:53:22");
        Timestamp d4=Timestamp.valueOf("2019-02-27 05:58:05");
        Timestamp d7=Timestamp.valueOf("2019-09-10 00:00:00");
        //List<Map<String, String>> VBVData=new ArrayList<>();
        //VBVData=k.getVBVData("J001",d1,d2);
        //List<Map<String, String>> rawDetectorData=new ArrayList<>();
        //rawDetectorData=k.getRawDetectorData("J001", d3, d4);
//        for (Map<String, String> row :rawDetectorData) {
//            System.out.println();
//            for (String e : row.keySet()) {
//                String key = e;
//                String value = row.get(e);
//                System.out.print(e + " : " + value + "\t");
//            }
//        }
////        TrafficData d = new TrafficData();
//        Timestamp d1= Timestamp.valueOf("2019-09-12 16:56:04");
//        Timestamp d2=Timestamp.valueOf("2019-09-19 16:56:04");

//        List<String> parameters= new ArrayList<String>();
//        parameters.add(d1.toString());
//        parameters.add(d2.toString());
//        List<HashMap<String, String>> VBVData=new ArrayList<>();
//        HashMap <String,String> mMap= new HashMap<>();
//        mMap.put("ext","vo");
//        mMap.put("start","yo");
//        mMap.put("tex","no");
//        mMap.put("te","go");
//        VBVData.add(mMap); 
// create a new one
//        int SiteID=k.getSiteId("J001");
//        System.out.println(SiteID);
       //List<Map<String, String>> Last30SecondData=k;
       //List<Map<String, String>> Last30SecondData=k.getDetectorData("aggregated","J014_L01_01",Timestamp.valueOf("2019-02-10 18:01:12"),Timestamp.valueOf("2019-02-10 19:21:12"));
        //List<Map<String, String>> Last30SecondData=k.getTrafficDynamicData();
        List<Map<String, String>> Last30Second=k.getTrafficSignalDefinitions();
        //String m=k.setSignalStage("J001", "1");
        for (Map<String, String> row :Last30Second) {
            System.out.println();
            for (String e : row.keySet()) {
                String key = e;
                String value = row.get(e);
                System.out.print(e + " : " + value + "\t");
            }
        }  
        
        List<Map<String, String>> Last30Second2=k1.getLast30SecondData("J001");
        //String m=k.setSignalStage("J001", "1");
        for (Map<String, String> row :Last30Second2) {
            System.out.println("Kamal");
            for (String e : row.keySet()) {
                String key = e;
                String value = row.get(e);
                System.out.print(e + " : " + value + "\t");
            }
        } 
//System.out.println(m);
//        for (Map<String, String> row :Last30Second) {
//            System.out.println();
//            for (String e : row.keySet()) {
//                String key = e;
//                String value = row.get(e);
//                System.out.print(e + " + " + value + "\t");
//            }
//        }
        k.disconnect();
    }
    
}
