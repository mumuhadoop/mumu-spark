package com.lovecws.mumu.spark.rdd.nginxlog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 解析nginx日志
 * @date 2017-10-12 12:06
 */
public class NginxAccessLogParser {

    /**
     * 解析日志文件中的一行
     *
     * @param readLine
     */
    public static Map<String, Object> parseLine(String readLine) {
        Map<String, Object> httpMap = new HashMap<String, Object>();
        //寻找ip地址
        String remoteIpAddress = getRemoteIpAddress(readLine);
        readLine = readLine.replace(remoteIpAddress, "");
        httpMap.put("remoteAddr", remoteIpAddress);

        String accessTime = getAccessTime(readLine);
        readLine = readLine.replace(accessTime, "");
        SimpleDateFormat sf = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.ENGLISH);
        try {
            httpMap.put("accessTime", sf.parse(accessTime));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //获取请求行
        String httpStatus = getHttpStatus(readLine);
        readLine = readLine.replace(httpStatus, "");
        String[] httpStatu = httpStatus.split(" ");

        if (httpStatu != null && httpStatu.length == 3) {
            httpMap.put("method", httpStatu[0]);
            httpMap.put("url", httpStatu[1]);
            httpMap.put("protocol", httpStatu[2]);
        }

        String httpMozilla = getHttpMozilla(readLine);
        readLine = readLine.replace(httpMozilla, "");
        httpMap.put("agent", httpMozilla);

        String httpRefer = getHttpRefer(readLine);
        httpMap.put("refer", httpRefer);
        readLine = readLine.replace(httpRefer, "");

        readLine = readLine.replace("\"", "").replace("-", "").trim();
        String[] strings = readLine.split(" ");
        if (strings != null && strings.length > 2) {
            httpMap.put("status", strings[0]);
            httpMap.put("length", strings[1]);
        }
        return httpMap;
    }

    /**
     * 获取remote ipaddress
     */
    private static final Pattern ipAddressPattern = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");

    public static String getRemoteIpAddress(String readLine) {
        Matcher matcher = ipAddressPattern.matcher(readLine);
        if (matcher.find()) {
            String group = matcher.group();
            return group;
        }
        return null;
    }

    /**
     * 获取访问时间
     */
    private static final Pattern accessTimePattern = Pattern.compile("\\[.*\\]");

    public static String getAccessTime(String readLine) {
        try {
            Matcher matcher = accessTimePattern.matcher(readLine);
            if (matcher.find()) {
                String group = matcher.group();
                /*group = group.replace("[", "");
                group = group.replace("]", "");
                SimpleDateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]", Locale.getDefault());
                System.out.println(dateFormat.parse(group));*/
                return group;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 获取http信息
     */
    private static final Pattern httpPattern = Pattern.compile("\".*/1.[1,0]\"");

    public static String getHttpStatus(String readLine) {

        Matcher matcher = httpPattern.matcher(readLine);
        if (matcher.find()) {
            String group = matcher.group();
            group = group.replace("\"", "");
            return group;
        }
        return "";
    }


    /**
     * 获取http信息
     */
    private static final Pattern referPattern = Pattern.compile("\"http.*\"");

    public static String getHttpRefer(String readLine) {
        Matcher matcher = referPattern.matcher(readLine);
        if (matcher.find()) {
            HashMap<String, String> httpMap = new HashMap<String, String>();
            String group = matcher.group();
            group = group.replace("\"", "");
            return group;
        }
        return "";
    }

    private static final Pattern MozillaPattern = Pattern.compile("\"Mozilla.*\"");
    private static final Pattern SpiderPattern = Pattern.compile("\"[a-zA-Z ]+ spider.*\"");

    public static String getHttpMozilla(String readLine) {
        Matcher matcher = MozillaPattern.matcher(readLine);
        if (matcher.find()) {
            String group = matcher.group();
            group = group.replace("\"", "");
            return group;
        }
        matcher = SpiderPattern.matcher(readLine);
        if (matcher.find()) {
            String group = matcher.group();
            group = group.replace("\"", "");
            return group;
        }
        return "";
    }
}
