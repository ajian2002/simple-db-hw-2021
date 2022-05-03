package simpledb.utils;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.logging.*;

public class LogPrint {
    static final Logger logger = Logger.getLogger("root");
    static String s;
    static int FILE = 1;

    static
    {
        FileHandler fileHandler = null;
        try
        {
            var f = new File("/tmp/simple-db/java%u.log");
            var p = f.getParentFile();
            if (!p.exists())
            {
                p.mkdirs();
            }
            fileHandler = new FileHandler(f.getAbsolutePath(), false);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        //创建一个SimpleFormatter，输出格式
        SimpleFormatter formatter = new SimpleFormatter();
        Formatter fff = new Formatter() {
            static String getLoggingProperty(String name) {
                return LogManager.getLogManager().getProperty(name);
            }

//            private final String format = "[%1$tl:%1$tM:%1$tS.%1$tN][%2$s]: %4$s: %5$s%6$s%n";
private final String format = "%5$s%6$s%n";

            @Override
            public String format(LogRecord record) {
                ZonedDateTime zdt = ZonedDateTime.ofInstant(record.getInstant(), ZoneId.systemDefault());
                String source;
                if (record.getSourceClassName() != null)
                {
                    source = record.getSourceClassName();
                    if (record.getSourceMethodName() != null)
                    {
                        source += " " + record.getSourceMethodName();
                    }
                }
                else
                {
                    source = record.getLoggerName();
                }
                String message = formatMessage(record);
                String throwable = "";
                if (record.getThrown() != null)
                {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    pw.println();
                    record.getThrown().printStackTrace(pw);
                    pw.close();
                    throwable = sw.toString();
                }
                return String.format(format, zdt, source, record.getLoggerName(), record.getLevel().getLocalizedName(), message, throwable);

            }
        };
        //设置formatter
        fileHandler.setFormatter(fff);
        //设置日志级别
        fileHandler.setLevel(Level.ALL);
        //把handler添加到logger
        logger.addHandler(fileHandler);
        //设置不使用父Logger的handler
        logger.setUseParentHandlers(false);
    }

    public synchronized static void print(String s) {
        print(FILE, s);
    }

    public synchronized static void print(int flag, String s) {
        if (flag == 1)
        {
            logger.info(s);
        }
        else if (flag == 2)
        {
            System.out.println(s);
        }
        else
        {
            ;
        }
    }
}
