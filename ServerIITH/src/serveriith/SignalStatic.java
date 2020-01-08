/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package serveriith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.Map;

/**
 * Class for acquiring traffic data from database.
 *
 * @author itspe
 */
public class SignalStatic {

    /**
     * Stores Connection to the database
     */
    public Connection con = null;
     public SignalStatic(Connection c){
         con=c;
     }
    /**
     * Runs SQL Query and retruns result in the form of HashMap
     *
     * @param query SQL query
     * @param args Optional arguments in case query requires values of
     * additional variables needs to be assigned
     * @return List of HashMap containing data recevied by running query.
     */
    private List<Map<String, String>> RunSQL(String query, Object... args) {
        List<Map<String, String>> ret = new ArrayList<>();
        try {
            PreparedStatement preparedQuery;
            preparedQuery = con.prepareStatement(query);
            int j = 1;
            for (Object s : args) {
                if (s instanceof String) {
                    preparedQuery.setString(j, (String) s);
                } else {
                    if (s instanceof Integer) {
                        preparedQuery.setInt(j, (int) s);
                    }
                }
                j++;
            }
            ResultSet rs3 = preparedQuery.executeQuery();
            ResultSetMetaData rsmd = rs3.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs3.next()) {
                int i = 1;
                Map<String, String> Result = new HashMap<>();
                while (i <= columnsNumber) {
                    Result.put(rsmd.getColumnName(i), rs3.getString(i));
                    i++;
                }
                ret.add(Result);
            }
        } catch (SQLException ex) {
            Logger.getLogger(TrafficData.class.getName()).log(Level.SEVERE,
                    ex.getMessage(), ex);
        }

        return ret;
    }

    /**
     * Gets Traffic Dynamic Data
     *
     * @return List of Map objects(corresponding to each row) containing the
     * data
     */
    public List<Map<String, String>> getTrafficSignalConfigurations() {
        List<Map<String, String>> TrafficSignalConfigurations = null;
        String query = "select * from `utmc_traffic_signal_config`";
        TrafficSignalConfigurations = this.RunSQL(query);
        return TrafficSignalConfigurations;
    }

    /**
     * Gets Traffic Signal Fault Data in List of HashMap
     *
     * @return List of Map objects
     */
    public List<Map<String, String>> getTrafficSignalDefinitions() {
        List<Map<String, String>> TrafficSignalDefinitions = null;
        String query = "select 'NMToken' as Coordinates,"
                + "'0' as Altitude,'WGS84' as GridType,"
                + "'http://www.livetrafficindia.com' as ImageLink,"
                + "SignalSCN as SystemCodeNumber,"
                + "utmc_freeflow_typeid_support_object.TypeDescription as"
                + " ObjectTypeRef,QualityStatementID as QualityRef,"
                + "CreationDate,LastUpdated,DeletionDate as HistoricDate,"
                + "utmc_device_subsystem_typeid.TypeDescription as DataSourceTypeRef,"
                + "Reality,ShortDescription,LongDescription,Colour,Longitude,"
                + "Latitude,'0' as Easting,'0' as Northing,"
                + "NetworkPathReference as NetworkPathRef,"
                + "LinkDistance from `utmc_traffic_signal_static` "
                + "inner join utmc_freeflow_typeid_support_object ON "
                + "utmc_freeflow_typeid_support_object.TypeID = "
                + "utmc_traffic_signal_static.TypeID "
                + "inner join utmc_device_subsystem_typeid ON "
                + "utmc_device_subsystem_typeid.TypeID = "
                + "utmc_traffic_signal_static.DataSource_TypeID";
        TrafficSignalDefinitions = this.RunSQL(query);
        return TrafficSignalDefinitions;
    }

}
