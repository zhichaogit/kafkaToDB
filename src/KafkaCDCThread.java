import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger; 
 
public class KafkaCDCThread extends Thread
{
    ConsumerServer        consumer = null;
    private static Logger log = Logger.getLogger(KafkaCDCThread.class);

    public KafkaCDCThread(ConsumerServer consumer_) 
    {
	consumer = consumer_;
    }

    public void run() 
    {
	log.info("KafkaCDCThread thread id [" + consumer.GetConsumerID() 
		 + "], start to run ...");
	Long     begin = new Date().getTime();
	Date     starttime = new Date();

	try {
	    consumer.ProcessMessages();
	} catch (Exception e) {
	    e.printStackTrace();		
	}
	
	Long end = new Date().getTime();
	Date endtime = new Date();
	Float use_time = ((float) (end - begin))/1000;
	DecimalFormat df = new DecimalFormat("####0.000");
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	log.info("Start Time: [" + sdf.format(starttime) 
		 + "], End Time: [" + sdf.format(endtime) 
		 + "], Run Time [" + df.format(use_time) + " s]");
    }

    public void close()
    {
	consumer.DropConsumer();
    }
}

