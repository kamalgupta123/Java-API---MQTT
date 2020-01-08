package serveriith;
/**
 *
 * @author Parag Raipuria
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sets up DB Link
 * @author itspe
 */
public class Dblink{
    /**
     *  Configuration file name.
     */
    final String configFile = "dbconfig.txt";
    
    /**
     * Path at which Configuration file exists.
     */
    String path;
    
    /**
     * Connection URL for Database connection.
     */
    String URL;
    
    /**
     * Username of Database.
     */
    String username;
    
    /**
     * password for Database.
     */
    String password;
    
    /**
     * Connection to Database.
     */
    Connection conn = null;
    
    
    /**
     * Configures the database according to dbconfig.txt.
     */
    public Dblink() {
        try {
            File current = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
            path = current.getParent();
        } catch (URISyntaxException ex) {
            Logger.getLogger(Dblink.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        File f = new File(path + "//" + configFile);
        //File f=new File(path+"/RLVDDBConfig.txt");
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            URL = br.readLine();
            username = br.readLine();
            password = br.readLine();

        } catch (IOException e) {
            Logger.getLogger(Dblink.class.getName()).log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * Sets up DB connection 
     * @return connection to the database
     */
    public Connection getConnection() {

        try {
            System.out.println("user:" + username);
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Dblink.class.getName()).log(Level.SEVERE,
                    ex.getMessage(), ex);
        }
        try {
            System.out.println(URL);
            conn = (Connection) DriverManager.getConnection(URL, 
                    username, password);
            Logger.getLogger(Dblink.class.getName()).log(Level.INFO, 
                    "Connection established");
        } catch (SQLException ex) {
            Logger.getLogger(Dblink.class.getName()).log(Level.SEVERE,
                    ex.getMessage(), ex);

        }
        return conn;
    }
}
