package com.binsearch.etl;

import cn.hutool.core.date.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.*;
import oshi.software.os.FileSystem;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Performance {

    Logger performance_log_ = LoggerFactory.getLogger("PERFORMANCE_LOG_");

    boolean stopFlag = true;

    int samplingTimeNum = 15;

    public static void main(String[] arge){
        new Performance().PerformanceInfo();
    }

    public void samplingTime(int time){
        this.samplingTimeNum = time;
    }

    public void stop(){
        this.stopFlag = false;
    }

    public void PerformanceInfo(){
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                SystemInfo si = new SystemInfo();
                OperatingSystem operatingSystem = si.getOperatingSystem();
                HardwareAbstractionLayer hardware = si.getHardware();
                Sensors sensors = hardware.getSensors();
                CentralProcessor processor = hardware.getProcessor();

                while(stopFlag){
                    try {
                        ArrayList<String> array = new ArrayList<String>();

                        // JVM
                        printJVM(array);

                        // CPU信息
                         printCpuInfo(processor, sensors, array);
                        // 磁盘
                        printSysFiles(operatingSystem, array);
                        // 运行内存
                        printMem(hardware.getMemory(), array);

//                      // 操作系统
//                      printSystemInfo(operatingSystem);

                        // 网络
                        printNetWork(hardware, array);

                        performance_log_.info("\r\n\r\n\r\n" + StringUtils.join(array, "\r\n"));

                        TimeUnit.SECONDS.sleep(5+random.nextInt(samplingTimeNum));
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        });

        thread.start();

    }

    public static void printCpuInfo(CentralProcessor processor, Sensors sensors,ArrayList<String> array) throws InterruptedException {
        array.add("=========================  CPU  ========================");
        long[] startTicks = processor.getSystemCpuLoadTicks();
        TimeUnit.MILLISECONDS.sleep(2000);
        long[] endTicks = processor.getSystemCpuLoadTicks();
        long user = endTicks[CentralProcessor.TickType.USER.getIndex()] - startTicks[CentralProcessor.TickType.USER.getIndex()];
        long nice = endTicks[CentralProcessor.TickType.NICE.getIndex()] - startTicks[CentralProcessor.TickType.NICE.getIndex()];
        long sys = endTicks[CentralProcessor.TickType.SYSTEM.getIndex()] - startTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long idle = endTicks[CentralProcessor.TickType.IDLE.getIndex()] - startTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long ioWait = endTicks[CentralProcessor.TickType.IOWAIT.getIndex()] - startTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long irq = endTicks[CentralProcessor.TickType.IRQ.getIndex()] - startTicks[CentralProcessor.TickType.IRQ.getIndex()];
        long softIrq = endTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - startTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
        long steal = endTicks[CentralProcessor.TickType.STEAL.getIndex()] - startTicks[CentralProcessor.TickType.STEAL.getIndex()];
        long totalCpu = user + nice + sys + idle + ioWait + irq + softIrq + steal;
        // CPU 温度（以摄氏度为单位）（如果有）
        array.add(String.format("CPU型号:%s --- CPU核心数:%d --- CPU温度:%s --- 总使用率:%s --- 系统使用率:%s --- 当前空闲率:%s --- 用户使用率:%s",
                        processor.getProcessorIdentifier().getName(),
                        processor.getLogicalProcessorCount(),
                        sensors.getCpuTemperature(),
                        Arith.round(Arith.mul(totalCpu, 100), 2),
                        Arith.round(Arith.mul(Arith.div(sys, totalCpu, 2), 100), 2),
                        Arith.round(Arith.mul(Arith.div(idle, totalCpu, 2), 100), 2),
                        Arith.round(Arith.mul(Arith.div(user, totalCpu, 2), 100), 2)
        ));
    }


    public static void printSysFiles(OperatingSystem os,ArrayList<String> arrays) {
        arrays.add("=========================  磁盘信息  ========================");
        FileSystem fileSystem = os.getFileSystem();
        List<OSFileStore> fileStores = fileSystem.getFileStores();
        for (OSFileStore fs : fileStores) {
            long free = fs.getUsableSpace();
            long total = fs.getTotalSpace();
            long used = total - free;
            arrays.add(String.format("盘符:%s --- 类型:%s --- 盘名:%s --- 总量:%s --- 剩余:%s --- 使用:%s --- 使用率:%s", fs.getMount(), fs.getType(), fs.getName()
                    , convertFileSize(total), convertFileSize(free), convertFileSize(used), Arith.mul(Arith.div(used, total, 2), 100)));
        }
    }

    private static void printSystemInfo(OperatingSystem operatingSystem) throws UnknownHostException {
        System.out.println("系统信息");
        OperatingSystem.OSVersionInfo versionInfo = operatingSystem.getVersionInfo();
        InetAddress ip = Inet4Address.getLocalHost();
        Properties properties = System.getProperties();
        System.out.println("主机名:" + ip.getHostName());
        /*  厂家 + 家族 + 版本 + 构建版号*/
        System.out.println("系统描述:" + operatingSystem.getManufacturer() + " " + operatingSystem.getFamily() + versionInfo.getVersion() + " Build " + versionInfo.getBuildNumber());
        System.out.println("进程运行数量:" + operatingSystem.getProcessCount());
        System.out.println("线程运行数量:" + operatingSystem.getThreadCount());
        System.out.println("系统位数:" + properties.getProperty("os.arch"));
        System.out.println("系统版本:" + properties.getProperty("os.version"));
        System.out.println("系统部署目录:" + properties.getProperty("user.dir"));
        System.out.println("系统IP:" + ip.getHostAddress());
        System.out.println("系统支持位数" + operatingSystem.getBitness());
        /**
         * 系统启动的大致时间，以秒为单位。
         */
        Date systemBootTime = new Date(operatingSystem.getSystemBootTime() * 1000);
        System.out.println("系统启动时间:" + DateUtil.format(systemBootTime, "yyyy-MM-dd HH:mm:ss"));
        /**
         * 自启动以来的秒数,运行时长
         */
        System.out.println("运行时长:" + secondToTime(operatingSystem.getSystemUptime()));
    }

    public static void printMem(GlobalMemory m,ArrayList<String> array) {
        array.add("=========================  系统内存信息  ========================");
        array.add(String.format("总内存:%s --- 已用内存:%s --- 剩余内存:%s" , convertFileSize(m.getTotal()),convertFileSize(m.getTotal() - m.getAvailable()),convertFileSize(m.getAvailable())));
    }

    public static void printJVM(ArrayList<String> arrays) {
        arrays.add("=========================  JVM  ========================");
        Properties props = System.getProperties();

        long maxMemory = Runtime.getRuntime().maxMemory();
        arrays.add(String.format("JVM最大可申请内存:%s",convertFileSize(maxMemory)));

        long totalMemory = Runtime.getRuntime().totalMemory();
        arrays.add(String.format("JVM当前可用的内存总量:%s",convertFileSize(totalMemory)));

        long freeMemory = Runtime.getRuntime().freeMemory();
        arrays.add(String.format("JVM空闲内存:%s",convertFileSize(freeMemory)));

        arrays.add(String.format("JVM内存使用率:%s",new DecimalFormat("#.##%").format((totalMemory - freeMemory) * 1.0 / totalMemory)));

        arrays.add(String.format("JVM最大内存容量:%s --- 版本:%s --- JAVA_HOME:%s",convertFileSize(maxMemory),props.getProperty("java.version"),props.getProperty("java.home")));
    }

    public static void printNetWork(HardwareAbstractionLayer hardware, ArrayList<String> array) {
        array.add("=========================  网络带宽  ========================");
        List<NetworkIF> networkIFs = hardware.getNetworkIFs();
        for (NetworkIF networkIF : networkIFs) {
            array.add(String.format("IP:%s --- 网络接收:%s --- 网络发送:%s --- 显示名称:%s --- MAC地址:%s",
                    Arrays.toString(networkIF.getIPv4addr()),
                    convertFileSize(networkIF.getBytesRecv()),
                    convertFileSize(networkIF.getBytesSent()),
                    networkIF.getDisplayName(),
                    networkIF.getMacaddr()));
        }
    }



    /**
     * 字节转换
     *
     * @param size 字节大小
     * @return 转换后值
     */
    public static String convertFileSize(long size) {
        long kb = 1024;
        long mb = kb * 1024;
        long gb = mb * 1024;
        if (size >= gb) {
            return String.format("%.1f GB", (float) size / gb);
        } else if (size >= mb) {
            float f = (float) size / mb;
            return String.format(f > 100 ? "%.0f MB" : "%.1f MB", f);
        } else if (size >= kb) {
            float f = (float) size / kb;
            return String.format(f > 100 ? "%.0f KB" : "%.1f KB", f);
        } else {
            return String.format("%d B", size);
        }
    }

    /**
     * 将秒数转换为日时分秒
     *
     * @param second
     * @return
     */
    public static String secondToTime(long second) {
        long days = second / 86400;            //转换天数
        second = second % 86400;            //剩余秒数
        long hours = second / 3600;            //转换小时
        second = second % 3600;                //剩余秒数
        long minutes = second / 60;            //转换分钟
        second = second % 60;                //剩余秒数
        if (days > 0) {
            return days + "天" + hours + "小时" + minutes + "分" + second + "秒";
        } else {
            return hours + "小时" + minutes + "分" + second + "秒";
        }
    }
}
