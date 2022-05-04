package com.iszhaoy.cli;

import org.apache.commons.cli.*;

import java.util.Locale;
import java.util.ResourceBundle;

public class CliTest {
    public static void main(String[] args) throws Exception {
        //args = new String[]{"-h"};
        simpleTest(args,Locale.CHINA);
        //simpleTest(args,Locale.ENGLISH);

        //args = new String[]{"-i","192.168.149.80","-p","8091","-t","http"};
        //simpleTest(args,Locale.CHINA);
        //simpleTest(args,Locale.ENGLISH);
    }


    public static void simpleTest(String[] args,Locale locale) {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("message",
                locale);
        Options opts = new Options();
        opts.addOption("h", false, resourceBundle.getString("HELP_DESCRIPTION"));
        opts.addOption("i", true, resourceBundle.getString("HELP_IPADDRESS"));
        opts.addOption("p", true, resourceBundle.getString("HELP_PORT"));
        opts.addOption("t", true, resourceBundle.getString("HELP_PROTOCOL"));
        BasicParser parser = new BasicParser();
        CommandLine cl;
        try {
            cl = parser.parse(opts, args);
            if (cl.getOptions().length > 0) {
                if (cl.hasOption('h')) {
                    HelpFormatter hf = new HelpFormatter();
                    hf.printHelp("Options", opts);
                } else {
                    String ip = cl.getOptionValue("i");
                    String port = cl.getOptionValue("p");
                    String protocol = cl.getOptionValue("t");

                    System.out.println(String.format("ip is %s\tport is %s\tprotocol is %s", ip,port,protocol ));
                    //if (!CIMServiceFactory.getinstance().isIPValid(ip)) {
                    //    System.err.println(resourceBundle.getString("INVALID_IP"));
                    //    System.exit(1);
                    //}
                    //try {
                    //    int rc = CIMServiceFactory.getinstance().rmdatasource(
                    //            ip, port, protocol);
                    //    if (rc == 0) {
                    //        System.out.println(resourceBundle
                    //                .getString("RMDATASOURCE_SUCCEEDED"));
                    //    } else {
                    //        System.err.println(resourceBundle
                    //                .getString("RMDATASOURCE_FAILED"));
                    //    }
                    //} catch (Exception e) {
                    //    System.err.println(resourceBundle
                    //            .getString("RMDATASOURCE_FAILED"));
                    //    e.printStackTrace();
                    //}
                }
            } else {
                System.err.println(resourceBundle.getString("ERROR_NOARGS"));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public static String getConfigPathParam(String[] args) throws Exception {
        final Options options = new Options();
        final Option option = new Option("f", true, "Configuration file path");
        options.addOption(option);

        final CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (final ParseException e) {
            throw new Exception("parser command line error", e);
        }

        String configFilePath = null;
        if (cmd.hasOption("f")) {
            configFilePath = cmd.getOptionValue("f");
        } else {
            System.err.println("please input the configuration file path by -f option");
            System.exit(1);
        }
        if (configFilePath == null || "".equals(configFilePath)) {
            throw new Exception("Blank file path");
        }

        System.out.println(configFilePath);
        return configFilePath;
    }
}
