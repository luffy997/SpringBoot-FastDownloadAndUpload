package com.dev.client;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 文件分片下载
 *
 * @author 路飞
 * @create 2021/3/3 16:37
 */
@RestController
public class DownLoadClient {
    private final static long PER_PAGE = 1024l * 1024l * 50l;
    private final static String DOWNPATH = "F:\\testUploadDownLoad"; //下载地址
    //为了方便，我们使用JDK自己的线程池
    ExecutorService pool = Executors.newFixedThreadPool(8);

    @RequestMapping("/downloadFile")
    public String downloadFile() throws Exception {
        //总分片数量
        FileInfo fileInfo = download(0, 10, -1, null);
        long pages = fileInfo.fSize / PER_PAGE;
        for (long i = 0; i <= pages; i++) {
            pool.submit(new Download(i * PER_PAGE, (i + 1) * PER_PAGE - 1, i, fileInfo.fName));
        }

        return "success";
    }

    //写一个内部类
    class FileInfo {
        long fSize;
        String fName;

        public FileInfo(long fSize, String fName) {
            this.fSize = fSize;
            this.fName = fName;
        }
    }

    class Download implements Runnable {

        long start;
        long end;
        long page;
        String fName;

        public Download(long start, long end, long page, String fName) {
            this.start = start;
            this.end = end;
            this.page = page;
            this.fName = fName;
        }

        @Override
        public void run() {
            try {
                FileInfo info = download(start, end, page, fName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private FileInfo download(long start, long end, long page, String fName) throws Exception {
        File file = new File(DOWNPATH, page + "-" + fName);
        if (file.exists()) {
            return null;
        }
        HttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://127.0.0.1:8080/download");
        httpGet.setHeader("Range", "bytes=" + start + "-" + end);

        HttpResponse response = client.execute(httpGet);
        HttpEntity entity = response.getEntity();
        InputStream is = entity.getContent();

        String fSize = response.getFirstHeader("fSize").getValue();
        fName = URLDecoder.decode(response.getFirstHeader("fName").getValue(), "utf-8");

        FileOutputStream fis = new FileOutputStream(file);
        byte[] buffer = new byte[1024];
        int ch = 0;
        while ((ch = is.read()) != -1) {
            fis.write(buffer, 0, ch);
        }
        is.close();
        fis.flush();
        fis.close();
        if (end - Long.valueOf(fSize) >= 0) { //最后一个分片就合并
            mergeFile(fName,page);
        }
        return new FileInfo(Long.valueOf(fSize), fName);
    }

    private void mergeFile(String fName, long page) throws Exception {
        File file = new File(DOWNPATH, fName);
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));

        for (long i = 0; i <= page; i++) {
            File tempFile = new File(DOWNPATH, i + "-" + fName);
            while (!tempFile.exists() || (i != page && tempFile.length() < PER_PAGE)) {
                Thread.sleep(100);
            }
            byte[] bytes = FileUtils.readFileToByteArray(tempFile);
            os.write(bytes);
            os.flush();
            tempFile.delete();
        }
        //删除探测文件
        File delFile = new File(DOWNPATH, -1 + "-null");
        delFile.delete();
        os.flush();
        os.close();
    }
}
