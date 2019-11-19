package com.esgyn.kafkaCDC.server.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;
import com.google.gson.Gson;

public class FileUtils {
    public final static int          BYTE_STRING         = 1;
    public final static int          SQL_STRING          = 2;

    private static Logger            log = Logger.getLogger(FileUtils.class);

    /**
     *  read paras from json file
     * @param inputPath
     * @return jsonString
     */
    private static String readJsonConf(String inputPath) {
        File jsonFile = new File(inputPath);
        if (jsonFile.exists() && jsonFile.isFile()) {
            FileInputStream fileInputStream;
            try {
                fileInputStream = new FileInputStream(jsonFile);
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                int ch=0;
                StringBuffer sb = new StringBuffer();
                while ((ch=inputStreamReader.read())!=-1) {
                    sb.append((char)ch);
                }
                inputStreamReader.close();
                return sb.toString();
            } catch (IOException e) {
                log.error("read jsonFile from conf/  cause an error",e);
            }
        }else{
            log.error("[" + inputPath + "] is not exists or not a file");
        }
        return null;
    }

    /**
     * Parse json formatted data via Gson
     * @param confPath
     * @return  Parameters
     */

    public static Parameters jsonParse(String confPath) throws Exception{
        Parameters params = null;
	String     jsonString = readJsonConf(confPath);

        try {
            Gson gson = new Gson();
	    params = gson.fromJson(jsonString, Parameters.class);
        } catch (Exception e) {
           throw e;
        }
        return params;
    }

    public static boolean createDirectory(String dirPath) {
	if (dirPath == null)
	    return true;

        File dir = new File(dirPath);

        if (dir.exists()) {
	    log.warn("directory [" + dirPath + "] is exist.");
            return true;
        }

        if (!dir.mkdirs()) {
	    log.error("create directory [" + dirPath + "] fail.");
            return false;
        }

	log.info("create directory [" + dirPath + "] succeed!");
	return true;
    }

    public static boolean dumpDataToFile(List<RowMessage> rows, String filePath , int type,
            int maxFileSize , int maxBackupIndex){
	FileOutputStream            output   = null;
	BufferedOutputStream        buffer   = null;
	boolean                     dumped   = true;

        if (log.isTraceEnabled()) { log.trace("enter"); }

        if (log.isDebugEnabled()) { 
	    log.debug("dump messages to file path [" + filePath + "]");
	}
		    
	try {
	    File file = new File(filePath);
	    if(!file.exists()) {
		file.createNewFile();
	    }

	    if (file.length() > maxFileSize) {
                rolloverFile(filePath,maxBackupIndex);
            }

	    output = new FileOutputStream(file, true);
	    buffer = new BufferedOutputStream(output);
	    for (RowMessage row : rows) {
		if (log.isDebugEnabled()) 
		{ 
		    log.debug("offset [" + row.getOffset() + "], message [" 
			     + row.getMsgString().getBytes() + "]"); 
		}

		String message = null;
		switch (type){
		case BYTE_STRING:
		    message = row.getMsgString();
		    break;

		case SQL_STRING:
		    message = row.toString();
		    break;

		default:
		    log.error("message type [" + type + "] is incorrect");
		    break;
		}
		
		if (message == null)
		    break;

		buffer.write(message.getBytes());
	    }
	} catch (FileNotFoundException fnfe) {
	    log.error("file [" + filePath + "] is not found  when dump, "
		      + "the message: ", fnfe);
	    dumped = false;
	} catch (IOException ioe) {
	    log.error("there are IOException when dump data to file [" 
		      + filePath + "], the message: ", ioe);
	    dumped = false;
	} catch (Exception e) {
	    log.error("there are Exception when dump data to file [" + filePath
		      + "], the message: ", e);
	    dumped = false;
	} finally {
	    if (buffer != null) {
		try {
		    buffer.flush();
		    buffer.close();
		} catch (Exception e) {
		    log.error("flush and close BufferedOutputStream exception: ", e);
		} finally {
		    buffer = null;
		}
	    }

	    if (output != null) {
		try {
		    output.close();
		} catch (Exception e) {
		    log.error("close FileOutputStream exception: ", e);
		} finally {
		    output = null;
		}
	    }

	    if (log.isTraceEnabled()) { 
		log.trace("exit, record [" + dumped + "]" );
	    }

	    return dumped;
	}
    }
    /**
     * Rollover the current file to a new file.
     * @param filePath
     */
    public static void rolloverFile(String filePath,int maxBackupIndex){
        File target;
        File file;
        if (log.isTraceEnabled()) { log.trace("enter"); }
        if (maxBackupIndex > 0) {
            // Delete the oldest file
            file = new File(filePath + ".bak" + maxBackupIndex);
            if (file.exists())
                file.delete();

            for (int i = maxBackupIndex - 1; i >= 1; i--) {
                file = new File(filePath + ".bak" + i);
                if (file.exists()) {
                    target = new File(filePath + ".bak" + (i + 1));
                    if (log.isDebugEnabled()) {
                        log.debug("Renaming file [" + file + "] to [" + target + "]");
                    }
                    file.renameTo(target);
                }
            }

            // Rename fileName to filename.bak1
            target = new File(filePath + ".bak" + 1);

            file = new File(filePath);
            if (log.isDebugEnabled()) {
                log.debug("Renaming file [" + file + "] to [" + target + "]");
            }
            file.renameTo(target);
        }else if (maxBackupIndex < 0){
            //find the max backup index
            for (int i = 1; i < Integer.MAX_VALUE; i++) {
                target = new File(filePath + ".bak" + i);
                if (! target.exists()) {
                    //Rename fileName to next index
                    file = new File(filePath);
                    file.renameTo(target);
                    if (log.isDebugEnabled()) {
                        log.debug("Renaming file [" + file + "] to [" + target + "]");
                    }
                    break;
                }
            }
        }
        if (log.isTraceEnabled()) { log.trace("exit"); }
    }
    /**
     * @param rootPathStr
     * @param startTime   save file start time
     * @param endTime    save file start end
     * @return
     */
    public static long saveTimeRegionFiles(String rootPathStr,Date startTime,Date endTime) {
        if (log.isTraceEnabled()) { log.trace("enter"); }
        File fullFilePath      = null;
        Date fileModifyTime    = null;
        StringBuffer strBuffer = null;
        File rootPath          = new File(rootPathStr);
        List<File> deleteFiles = new ArrayList<>();
        rootPathStr            = rootPathStr.endsWith("/") ? rootPathStr : (rootPathStr + "/");

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
            strBuffer.append("current delete " + rootPathStr + " files[");
        }
        for (String fileName : rootPath.list()) {
            fullFilePath   = new File(rootPathStr +"/"+ fileName);
            fileModifyTime = new Date(fullFilePath.lastModified());
            if (fileModifyTime.before(endTime) || fileModifyTime.after(startTime)) {
                if (log.isDebugEnabled()) {
                    strBuffer.append(fileName + ",");
                }
                deleteFiles.add(fullFilePath);
            }
        }

        long dropFileCount= batchDropFiles(deleteFiles);

        if (log.isDebugEnabled()) {
            if (dropFileCount > 0) {
                log.debug("current delete files num [" + dropFileCount + "]");
                strBuffer.append("]");
                log.debug(strBuffer.toString());
            }else {
                log.debug("no file need to drop.");
            }
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
        return dropFileCount;
    }
    /**
     *  batch drop files
     * @param deleteFiles  file List for drop
     * @return dropFileCount drop File number
     */
    public static long batchDropFiles(List<File> deleteFiles) {
        for (File file : deleteFiles) {
            file.delete();
        }
        return deleteFiles.size();
    }
}
