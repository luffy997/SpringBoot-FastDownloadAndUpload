package com.dev.controller;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.List;

/**
 * 文件上传
 * @author 路飞
 * @create 2021/3/3 10:46
 */
@RestController
public class UploadController {

    private static final String UTF8 = "utf-8";

    @RequestMapping("/upload")
    public void upload(HttpServletRequest request, HttpServletResponse response) throws Exception {

        //分片
        response.setCharacterEncoding(UTF8);
        Integer schunk = null;
        Integer schunks = null;
        String name = null;
        String uploadPath = "F:\\testUploadDownLoad";
        BufferedOutputStream os = null;
        try{
            DiskFileItemFactory factory = new DiskFileItemFactory();
            factory.setSizeThreshold(1024);
            factory.setRepository(new File(uploadPath));
            ServletFileUpload upload = new ServletFileUpload(factory);
            upload.setFileSizeMax(5l *1024l *1024l*1024l);
            upload.setSizeMax(10l *1024l *1024l*1024l);
            List<FileItem> items = upload.parseRequest(request);

            for(FileItem item : items){
                if(item.isFormField()){
                    if("chunk".equals(item.getFieldName())){
                        schunk = Integer.parseInt(item.getString(UTF8));
                    }
                    if("chunks".equals(item.getFieldName())){
                        schunks = Integer.parseInt(item.getString(UTF8));
                    }
                    if("name".equals(item.getFieldName())){
                        name = item.getString(UTF8);
                    }
                }
            }
            for(FileItem item : items){
                if(!item.isFormField()){
                    String temFileName = name;
                    if(name != null){
                        if(schunk != null){
                            temFileName = schunk +"_"+name;
                        }
                        File temFile = new File(uploadPath,temFileName);
                        if(!temFile.exists()){//断点续传
                            item.write(temFile);
                        }
                    }
                }
            }

            //文件合并
            if(schunk != null && schunk.intValue() == schunks.intValue()-1){
                File tempFile = new File(uploadPath,name);
                os = new BufferedOutputStream(new FileOutputStream(tempFile));

                for(int i=0 ;i<schunks;i++){
                    File file = new File(uploadPath,i+"_"+name);
                    while(!file.exists()){ //多线程环境下很可能后面的写完了，前面的没写完，所以要等下，直到file写完
                        Thread.sleep(100);
                    }
                    byte[] bytes = FileUtils.readFileToByteArray(file);
                    os.write(bytes);
                    os.flush();
                    file.delete();
                }
                os.flush();
            }

            response.getWriter().write("上传成功"+name);
        }finally {
            try{
                if(os != null){
                    os.close();
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}

