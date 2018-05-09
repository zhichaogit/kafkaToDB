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
	log.info("start to run ...");

	try {
	    consumer.ProcessMessages();
	} catch (Exception e) {
	    e.printStackTrace();		
	}
	log.info("stoped.");
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

