package com.binsearch.etl.ftp;

import com.jcraft.jsch.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.logging.log4j.util.Strings;

import javax.transaction.SystemException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;

public class SFtpFactory implements PooledObjectFactory<ChannelSftp> {

    String username;
    String host;
    int port;
    String password;



    public static void upload(byte[] file, String remotName, String remotePath,ChannelSftp sftp)throws Exception {
        try {
            if (file.length > 0) {
                String rpath = remotePath; // 服务器需要创建的路径
                try {
                    createDir(rpath, sftp);
                } catch (Exception e) {
                    throw new Exception("创建路径失败：" + rpath);
                }
                // this.sftp.rm(file.getName());
                sftp.cd(remotePath);
                sftp.put(new ByteArrayInputStream(file), remotName);
            }
        } catch (FileNotFoundException e) {
            throw new SystemException("上传文件没有找到");
        } catch (SftpException e) {
            throw new SystemException("上传ftp服务器错误");
        }
    }


    public static boolean download(String filePath,String sourceFilePath,ChannelSftp sftp)throws Exception{
        ByteArrayOutputStream byteArrayOutputStream = null;
        try {
            File file = new File(filePath);
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }

            byteArrayOutputStream = new ByteArrayOutputStream();
            sftp.get(sourceFilePath, byteArrayOutputStream);
            FileUtils.writeByteArrayToFile(file,byteArrayOutputStream.toByteArray());
        } catch (Exception e) {
            throw e;
        }
        return true;
    }

    public static void createDir(String createpath, ChannelSftp sftp) throws Exception{
        try {
            if (isDirExist(sftp,createpath)) {
                sftp.cd(createpath);
                return;
            }
            String pathArry[] = createpath.split("/");
            StringBuffer filePath = new StringBuffer("/");
            for (String path : pathArry) {
                if (path.equals("")) {
                    continue;
                }
                filePath.append(path + "/");
                if (isDirExist(sftp,filePath.toString())) {
                    sftp.cd(filePath.toString());
                } else {
                    // 建立目录
                    sftp.mkdir(filePath.toString());
                    // 进入并设置为当前目录
                    sftp.cd(filePath.toString());
                }
            }
            sftp.cd(createpath);
        } catch (SftpException e) {
            throw new SystemException("创建路径错误：" + createpath);
        }
    }

    public static boolean isFileExist(ChannelSftp channelSftp,
                                String fileDir,
                                String fileName) {
        if (!isDirExist(channelSftp, fileDir)) {
            return false;
        }
        try{
            Vector vector = channelSftp.ls(fileDir);
            for (int i = 0; i < vector.size(); ++i) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) vector.get(i);
                if (fileName.equals(entry.getFilename())) {
                    return true;
                }
            }
        }catch (Exception e) {
            return false;
        }
        return false;
    }

    public static boolean isDirExist(ChannelSftp sftp, String directory) {
        boolean isDirExistFlag = false;
        try {
            SftpATTRS sftpATTRS = sftp.lstat(directory);
            isDirExistFlag = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                isDirExistFlag = false;
            }
        }
        return isDirExistFlag;
    }

    public SFtpFactory(String host, int port, String username, String password){
        this.username = username;
        this.host = host;
        this.port = port;
        this.password = password;
    }

    @Override
    public PooledObject<ChannelSftp> makeObject() throws Exception {
        JSch jsch = new JSch();
        Session sshSession = jsch.getSession(username, host, port);

        if(Strings.isNotEmpty(password)) {
            sshSession.setPassword(password);
        }

        Properties sshConfig = new Properties();
        sshConfig.put("StrictHostKeyChecking", "no");
        sshSession.setConfig(sshConfig);
        sshSession.connect();

        Channel channel = sshSession.openChannel("sftp");
        channel.connect();

        return new DefaultPooledObject<>( (ChannelSftp) channel);
    }

    @Override
    public void destroyObject(PooledObject<ChannelSftp> p) throws Exception {
        //断开连接
        p.getObject().disconnect();
    }

    @Override
    public boolean validateObject(PooledObject<ChannelSftp> p) {
        //判断这个对象是否是保持连接状态
        return p.getObject().isConnected();
    }

    @Override
    public void activateObject(PooledObject<ChannelSftp> p) throws Exception {
        //激活这个对象
    }

    @Override
    public void passivateObject(PooledObject<ChannelSftp> p) throws Exception {
        //不处理
    }
}
