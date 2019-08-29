package com.esgyn.kafkaCDC.server.kafkaConsumer;

import org.apache.log4j.Logger;

import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.databaseLoader.LoadStates;
import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerTasks;

import lombok.Getter;
import lombok.Setter;

public class ConsumeStates {
    @Getter
    private Parameters                  params        = null;
    @Getter
    private LoadStates                  loadStates    = null;
    @Getter
    private ConsumerTasks               consumerTasks = null;

    private long                        kafkaMsgNum   = 0;
    private long                        incMsgNum     = 0;

    private long                        insMsgNum     = 0;
    private long                        updMsgNum     = 0;
    private long                        keyMsgNum     = 0;
    private long                        delMsgNum     = 0;

    private long                        maxSpeed      = 0;

    private Date                        startTime     = null;

    private static Logger log = Logger.getLogger(ConsumeStates.class);

    public ConsumeStates(ConsumerTasks consumerTasks_, LoadStates loadStates_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	consumerTasks = consumerTasks_;
	loadStates    = loadStates_;
	params        = consumerTasks.getParams();

        startTime     = new Date();

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public synchronized void addInsMsgNum(long insMsgNum_) {
        insMsgNum += insMsgNum_;
    }

    public synchronized void addUpdMsgNum(long updMsgNum_) {
        updMsgNum += updMsgNum_;
    }

    public synchronized void addKeyMsgNum(long keyMsgNum_) {
        keyMsgNum += keyMsgNum_;
    }

    public synchronized void addDelMsgNum(long delMsgNum_) {
        delMsgNum += delMsgNum_;
    }

    public synchronized void addKafkaMsgNum(long kafkaMsgNum_) {
        kafkaMsgNum += kafkaMsgNum_;
	incMsgNum   += kafkaMsgNum_;
    }

    public void show(StringBuffer strBuffer) {
	long interval = params.getKafkaCDC().getInterval();
        Date endTime = new Date();
        Float useTime = ((float) (endTime.getTime() - startTime.getTime())) / 1000;
        long avgSpeed = (long) (kafkaMsgNum / useTime);
        long curSpeed = (long) (incMsgNum / (interval / 1000));
        if (curSpeed > maxSpeed)
            maxSpeed = curSpeed;
        DecimalFormat df = new DecimalFormat("####0.000");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	strBuffer.append("  running time [" + df.format(useTime) + "s")
	    .append(", start: " + sdf.format(startTime))
	    .append(", cur: " + sdf.format(endTime) + "]\n")

	    .append("  Consumers states [total: " + kafkaMsgNum)
	    .append(", inc: " + incMsgNum + "]")
	    .append(", messages [I: " + insMsgNum)
	    .append(", U: " + updMsgNum)
	    .append(", K: " + keyMsgNum)
	    .append(", D: " + delMsgNum + "]")
	    .append(", Speed(n/s) [max: " + maxSpeed)
	    .append(", avg: " + avgSpeed)
	    .append(", cur: " + curSpeed + "]\n");

	incMsgNum = 0;
    }
}
