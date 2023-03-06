package com.binsearch.etl.ftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.transaction.SystemException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.Objects;

public class FtpObjectPool {
    GenericObjectPool<ChannelSftp> objectPool;
    FtpProp prop;
    public FtpObjectPool(FtpProp prop) {
        this.prop = prop;
    }

    public FtpObjectPool init(){
        if(prop.getSFtpEnable() && Objects.isNull(objectPool)) {
            //设置对象池的相关参数
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            poolConfig.setMaxIdle(30);
            poolConfig.setMaxTotal(30);
            poolConfig.setMinIdle(2);


            //新建一个对象池,传入对象工厂和配置
            objectPool = new GenericObjectPool<ChannelSftp>(
                    new SFtpFactory(
                            prop.getIp(),Integer.parseInt(prop.getPort()),
                            prop.getUser(),prop.getPwd()),
                    poolConfig
            );
        }
        return this;
    }


    //从linux上下载文件
    //sourceFile 源文件路径
    //targetFile 目标文件路径
    public File download(String sourceFile, String targetFile) throws Exception {
        //windows 先下载到本地，再获取文件的本地路径
        File localFile;
        ChannelSftp channelSftp = null;
        FileOutputStream fileOutputStream = null;
        try {
            channelSftp = objectPool.borrowObject();
            if (Objects.isNull(channelSftp.lstat(sourceFile))) {
                throw new Exception("下载源文件不存在:"+sourceFile);
            }

            localFile = new File(targetFile);
            if(!localFile.getParentFile().exists()){
                localFile.getParentFile().mkdirs();
            }

            fileOutputStream = new FileOutputStream(localFile);
            channelSftp.get(sourceFile, fileOutputStream);
        }catch (Exception e){
            throw e;
        }
        finally {
            if(Objects.nonNull(fileOutputStream)){
                fileOutputStream.flush();
                fileOutputStream.close();
            }

            if (Objects.nonNull(channelSftp)) {
                objectPool.returnObject(channelSftp);
            }
        }
        return localFile;
    }

    public void upload(byte[] bys, String filePath)throws Exception {
        if(prop.getSFtpEnable()) {
            ChannelSftp channelSftp = null;
            try {
                channelSftp = objectPool.borrowObject();
                if (bys.length > 0) {
                    File file =  new File(filePath);
                    String rpath = file.getParent().replace("\\","/"); // 服务器需要创建的路径
                    try {
                        createDir(rpath);
                    } catch (Exception e) {
                        throw new Exception("创建路径失败：" + rpath);
                    }
                    // this.sftp.rm(file.getName());
                    channelSftp.cd(rpath);
                    channelSftp.put(new ByteArrayInputStream(bys), file.getName());
                }
            } catch (Exception e) {
                throw new Exception("上传文件没有找到");
            }finally{
                if(Objects.nonNull(channelSftp)){
                    objectPool.returnObject(channelSftp);
                }
            }
            return;
        }
        FileUtils.writeByteArrayToFile(new File(filePath), bys);
    }

    public boolean createDir(String createpath) throws Exception{
        if(prop.getSFtpEnable()) {
            boolean b = false;
            ChannelSftp channelSftp = null;
            try {
                channelSftp = objectPool.borrowObject();
                if (isDirExist(createpath)) {
                    channelSftp.cd(createpath);
                    return true;
                }
                String pathArry[] = createpath.split("/");
                StringBuffer filePath = new StringBuffer("/");
                for (String path : pathArry) {
                    if (path.equals("")) {
                        continue;
                    }
                    filePath.append(path + "/");
                    if (isDirExist(filePath.toString())) {
                        channelSftp.cd(filePath.toString());
                    } else {
                        // 建立目录
                        channelSftp.mkdir(filePath.toString());
                        // 进入并设置为当前目录
                        channelSftp.cd(filePath.toString());
                    }
                }
                channelSftp.cd(createpath);
                b = true;
            } catch (SftpException e) {
                b = false;
                throw new SystemException("创建路径错误：" + createpath);
            }finally {
                if(Objects.nonNull(channelSftp)){
                    objectPool.returnObject(channelSftp);
                }
            }
            return b;
        }
        File temp = new File(createpath);
        if(!temp.exists()||temp.isFile()){
            return temp.mkdirs();
        }
        return true;
    }

    public boolean isDirExist(String directory) {
        if(prop.getSFtpEnable()) {
            boolean isDirExistFlag = false;
            ChannelSftp channelSftp = null;
            try {
                channelSftp = objectPool.borrowObject();
                SftpATTRS sftpATTRS = channelSftp.lstat(directory);
                isDirExistFlag = true;
                return sftpATTRS.isDir();
            } catch (Exception e) {
                if (e.getMessage().toLowerCase().equals("no such file")) {
                    isDirExistFlag = false;
                }
            }finally {
                if(Objects.nonNull(channelSftp)){
                    objectPool.returnObject(channelSftp);
                }
            }
            return isDirExistFlag;
        }
        File stemp  = new File(directory);
        return stemp.exists() && !stemp.isFile();
    }

    public boolean isFileExist(String sourceFile){
        if(prop.getSFtpEnable()){
            ChannelSftp channelSftp = null;
            boolean exists = false;
            try {
                channelSftp = objectPool.borrowObject();
                if (Objects.isNull(channelSftp.lstat(sourceFile))) {
                    throw new Exception("文件不存在:"+sourceFile);
                }
                exists = true;
            }catch (Exception e){
                exists = false;
            }finally {
                if (Objects.nonNull(channelSftp)) {
                    objectPool.returnObject(channelSftp);
                }
            }
            return exists;
        }
        return new File(sourceFile).exists();
    }

    //获取文件路径
    public String getSourceFilePath(String sourceFilePath,String targetFile){
        if(prop.getSFtpEnable()) {
            //windows 先下载到本地，再获取文件的本地路径
            File file = null;
            try {
                file = download(sourceFilePath, targetFile);
            }catch (Exception e){}
            return Objects.nonNull(file)?file.getAbsolutePath():null;
        }
        // linux 判断是否存在
        return new File(sourceFilePath).exists()?sourceFilePath:null;
    }

    //获取文件文本
    public String getSourceFileText(String sourceFilePath,String targetFile) {
        String text = null;
        File file = null;
        try {
            if (prop.getSFtpEnable()) {
                //windows 先下载到本地，再获取文件的本地路径
                file = download(sourceFilePath, targetFile);
            } else {
                // Linux 直接获取文本
                file = new File(sourceFilePath);
            }
            if (Objects.isNull(file)||!file.exists()) {
                return null;
            }
            text = FileUtils.readFileToString(file, Charset.defaultCharset());
        }catch (Exception e){}
        return text;
    }


    public void stop(){}


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FtpProp{
        private String ip;

        private String port;
        private String user;
        private String pwd;

        private Boolean sFtpEnable;
    }

}
