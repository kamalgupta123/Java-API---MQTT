/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clientiith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray; 
import org.json.simple.JSONObject; 
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
  

/**
 *
 * @author itspe
 */
public class JSON {
    public class Message{
        public String function = null;
        public List<Map<String, String>> table = null;
        public List<String> parameters = null;
        public String ipAddress = null;
        public String UUID=null;    
    }
    
    /**
     * It takes json string as input and pick out the values of variables present in json string and        store those variable values into respective variable
     * @param json string
     * @return a Message Class objects which has variable which contains values parsed from json string
     */
    
    public Message decode(String data){
        Message ret = new Message();
        try {
            JSONObject jo = (JSONObject) new JSONParser().parse(data);
            ret.function = (String) jo.get("function");
            ret.ipAddress = (String) jo.get("ipAddress");
            ret.UUID = (String) jo.get("UUID");
            JSONArray param = (JSONArray) jo.get("parameters"); 
            if (param != null){
                Iterator itr = param.iterator(); 
                ret.parameters = new ArrayList<String>();
                while(itr.hasNext()){
                    ret.parameters.add((String) itr.next());
                }
            }
            JSONArray rows = (JSONArray) jo.get("table"); 
            if (rows != null){
                ret.table = new ArrayList<Map<String, String>>(); 
                Iterator itr = rows.iterator(); 
                while (itr.hasNext())  
                { 
                    ret.table.add((Map) itr.next());
                }
            }            
        } catch (ParseException ex) {
            Logger.getLogger(JSON.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }
    
    /**
     * It takes message class object as input.It takes out the values from variables of message class         object and make a json string of those variable's values.
     * @param mess, A message class object
     * @return json string
     */
    public String encode(Message mess){
        JSONObject jmess = new JSONObject();
        jmess.put("function", mess.function);
        jmess.put("ipAddress", mess.ipAddress);
        jmess.put("UUID", mess.UUID);
        JSONArray jparam = new JSONArray();
        if(mess.parameters!=null){
        for(String obj : mess.parameters){
            jparam.add(obj);
        }
        }
        jmess.put("parameters", jparam);
        JSONArray jtable = new JSONArray();
        if(mess.table!=null){
           for (Map<String, String> row : mess.table) {
                jtable.add(row);
            }
            jmess.put("table", jtable);
        }
        return jmess.toJSONString();
    }
}


