package main.java.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler to write to and read from auxiliar file.
 */
public class AuxiliarFileHandler {

    public static void writeToFile(String file_path, long start_ts, long final_ts) {
        OutputStream out = null;
        try {
            Properties props = new Properties();

            // To be read later in the Clock class
            props.setProperty("startLoadTimestamp", "" + start_ts);
            props.setProperty("lastLoadTimestamp", "" + final_ts);

            // Prepare the output file
            File file = new File(file_path + "/htapb_auxiliar");
            out = new FileOutputStream(file);

            // Write to the file
            props.store(out, "HTAPB auxiliar file");
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static long importLastTs(String file_path) {
        long result = 0L;
        try {
            Properties props = new Properties();

            // Import the properties of the file
            File file = new File(file_path + "/htapb_auxiliar");
            InputStream input = new FileInputStream(file);

            // Load the properties
            props.load(input);

            // Get the requested timestamp
            String res = props.getProperty("lastLoadTimestamp");
            result = Long.parseLong(res);
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

        return result;
    }

    public static long importFirstTs(String file_path) {
        long result = 0L;
        try {
            Properties props = new Properties();

            // Import the properties of the file
            File file = new File(file_path + "/htapb_auxiliar");
            InputStream input = new FileInputStream(file);

            // Load the properties
            props.load(input);

            // Get the requested timestamp
            String res = props.getProperty("startLoadTimestamp");
            result = Long.parseLong(res);
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

        return result;
    }
}
