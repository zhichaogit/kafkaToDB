package com.esgyn.kafkaCDC.client;

import com.esgyn.kafkaCDC.server.clientServer.KCConnection;
import com.esgyn.kafkaCDC.server.clientServer.Message;
import com.esgyn.kafkaCDC.server.utils.Constants;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class KCClient {
    private static String         host    = null;
    private static String         port    = null;
    private static String         type    = null;
    private static String         subType = null;

    public static void main(String[] args) {
        KCClient kcClient = new KCClient();

        try {
            kcClient.init(args);
        } catch (ParseException e1) {
            System.err.println("init_args faild."+e1);
        }

	kcClient.process();
    }

    public void init(String[] args) throws ParseException  {
	Option  option     = null;
        Options exeOptions = new Options();

	for (Object[] param : Constants.CLIENT_CONFIG_PARAMS){
            String   opt      = (String)param[0];
            String   longOpt  = (String)param[1];
            Boolean  required = (Boolean)param[2];
            Boolean  hasArg   = (Boolean)param[3];
            String   desc     = (String)param[4];

	    if (opt.equals("")){
		if (hasArg)
		    option = Option.builder().longOpt(longOpt).required(required)
			.hasArg().desc(desc).build();
		else
		    option = Option.builder().longOpt(longOpt).required(required)
			.desc(desc).build();
	    } else {
		if (hasArg)
		    option = Option.builder(opt).longOpt(longOpt).required(required)
			.hasArg().desc(desc).build();
		else
		    option = Option.builder(opt).longOpt(longOpt).required(required)
			.desc(desc).build();
	    }
	    
	    exeOptions.addOption(option);
        }

        CommandLine   cmdLine = null;
	try{
	    DefaultParser parser  = new DefaultParser();
	    cmdLine = parser.parse(exeOptions, args);
	} catch (Exception e) {
	    System.err.println("KafkaCDC client init parameters error." + e);
            System.exit(0);
	}

        boolean getVersion = cmdLine.hasOption("version") ? true : false;
        if (getVersion) {
            System.out.println("KafkaCDC current version is: " + Constants.KafkaCDC_VERSION);
            System.exit(0);
        }

        boolean getHelp = cmdLine.hasOption("help") ? true : false;
        if (getHelp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("KafkaCDC Client", exeOptions);
            System.exit(0);
        }


        host     = cmdLine.hasOption("host") ? cmdLine.getOptionValue("host") : "localhost";
        port     = cmdLine.hasOption("port") ? cmdLine.getOptionValue("port") : "8889";
 	type     = cmdLine.hasOption("type") ? cmdLine.getOptionValue("type") : "";
	subType  = cmdLine.hasOption("subType") ? cmdLine.getOptionValue("subType") : "";
    }

    public void process () {
        try {
            KCConnection kcConnection = new KCConnection(host, port);

            Message reqMessage = new Message();
	    if (reqMessage.init(type, subType)) {
		kcConnection.send(reqMessage);

		Message msg = kcConnection.receive();

		System.out.println(msg.getMsgs());
	    }
        } catch (Exception e) {
            e.printStackTrace();
        } 
    }
}

