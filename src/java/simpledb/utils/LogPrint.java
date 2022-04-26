package simpledb.utils;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LogPrint {
    static final Logger logger = Logger.getLogger("root");
    static String s;
    static boolean FILE = true;

    static
    {
        FileHandler fileHandler = null;
        try
        {
            fileHandler = new FileHandler("/tmp/simple-db/java%u.log", false);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        //创建一个SimpleFormatter，输出格式
        SimpleFormatter formatter = new SimpleFormatter();
        //设置formatter
        fileHandler.setFormatter(formatter);
        //设置日志级别
        fileHandler.setLevel(Level.ALL);
        //把handler添加到logger
        logger.addHandler(fileHandler);
        //设置不使用父Logger的handler
        logger.setUseParentHandlers(false);
    }

    public synchronized static void print(String s) {
        if (FILE)
        {
            logger.info(s);
        }
        else
        {
            System.out.println(s);
        }
    }
}
