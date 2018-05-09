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

	try {
	    consumer.ProcessMessages();
	} catch (Exception e) {
	    e.printStackTrace();		
	}
	log.info("KafkaCDCThread stoped.");
    }

    public int threadid()
    {
	return consumer.GetConsumerID();
    }

    public void close()
    {
	consumer.DropConsumer();
    }
}

