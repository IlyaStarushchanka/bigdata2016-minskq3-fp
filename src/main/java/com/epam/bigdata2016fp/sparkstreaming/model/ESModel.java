package com.epam.bigdata2016fp.sparkstreaming.model;

import eu.bitwalker.useragentutils.UserAgent;
import org.json.JSONObject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ESModel implements Serializable {
    private String bidId;
    private String timestamp;
    private String iPinyouId;

    private String device;
    private String osName;
    private String uaFamily;
    private CityInfo geoPoint;
    private String ip;
    private int region;
    private int city;
    private int addExchange;
    private String domain;
    private String url;
    private String anonUrl;
    private String addSlot;
    private int addSlotWidth;
    private int addSlotHeight;
    private int addSlotVisability;
    private int addSlotFormat;
    private int payingPrice;
    private String creativeId;
    private int biddingPrice;
    private String advertiserId;
    private String userTags;
    private int streamId;

    private int mlResult;

    private static final SimpleDateFormat LOGS_DATE_FORMAT = new SimpleDateFormat("yyyyMMddhhmmss");
    private static final SimpleDateFormat JSON_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

    public String toStringifyJson() {
        JSONObject jo = new JSONObject(this);
        jo.append("@sended_at", new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(new Date()));
        return jo.toString();
    }

    public static ESModel parseLine(String line) {
        try {
            String[] arr = line.split("\\t");
            ESModel esModel = new ESModel();
            esModel.bidId = arr[0];
            //Convert date time
            Date date = LOGS_DATE_FORMAT.parse(arr[1]);
            esModel.timestamp = JSON_DATE_FORMAT.format(date);
            // -----
            esModel.iPinyouId = arr[2];
            //User agent parsing
            UserAgent ua = UserAgent.parseUserAgentString(arr[3]);
            esModel.device = ua.getOperatingSystem().getDeviceType().getName();
            esModel.osName = ua.getOperatingSystem().getName();
            esModel.uaFamily = ua.getBrowser().getGroup().getName();
            //-----
            esModel.ip = arr[4];
            esModel.region = Integer.parseInt(arr[5]);
            esModel.city = Integer.parseInt(arr[6]);
            esModel.addExchange = Integer.parseInt(arr[7]);
            esModel.domain = arr[8];
            esModel.url = arr[9];
            esModel.anonUrl = arr[10];
            esModel.addSlot = arr[11];
            esModel.addSlotWidth = Integer.parseInt(arr[12]);
            esModel.addSlotHeight = Integer.parseInt(arr[13]);
            esModel.addSlotVisability = Integer.parseInt(arr[14]);
            esModel.addSlotFormat = Integer.parseInt(arr[15]);
            esModel.payingPrice = Integer.parseInt(arr[16]);
            esModel.creativeId = arr[17];
            esModel.biddingPrice = Integer.parseInt(arr[18]);
            esModel.advertiserId = arr[19];
            esModel.userTags = arr[20];
            esModel.streamId = Integer.parseInt(arr[21]);
            return esModel;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    //GETTERS AND SETTERS

    public String getBidId() {
        return bidId;
    }

    public void setBidId(String bidId) {
        this.bidId = bidId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(String iPinyouId) {
        this.iPinyouId = iPinyouId;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getUaFamily() {
        return uaFamily;
    }

    public void setUaFamily(String uaFamily) {
        this.uaFamily = uaFamily;
    }

    public CityInfo getGeoPoint() {
        return geoPoint;
    }

    public void setGeoPoint(CityInfo geoPoint) {
        this.geoPoint = geoPoint;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getRegion() {
        return region;
    }

    public void setRegion(int region) {
        this.region = region;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public int getAddExchange() {
        return addExchange;
    }

    public void setAddExchange(int addExchange) {
        this.addExchange = addExchange;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAnonUrl() {
        return anonUrl;
    }

    public void setAnonUrl(String anonUrl) {
        this.anonUrl = anonUrl;
    }

    public String getAddSlot() {
        return addSlot;
    }

    public void setAddSlot(String addSlot) {
        this.addSlot = addSlot;
    }

    public int getAddSlotWidth() {
        return addSlotWidth;
    }

    public void setAddSlotWidth(int addSlotWidth) {
        this.addSlotWidth = addSlotWidth;
    }

    public int getAddSlotHeight() {
        return addSlotHeight;
    }

    public void setAddSlotHeight(int addSlotHeight) {
        this.addSlotHeight = addSlotHeight;
    }

    public int getAddSlotVisability() {
        return addSlotVisability;
    }

    public void setAddSlotVisability(int addSlotVisability) {
        this.addSlotVisability = addSlotVisability;
    }

    public int getAddSlotFormat() {
        return addSlotFormat;
    }

    public void setAddSlotFormat(int addSlotFormat) {
        this.addSlotFormat = addSlotFormat;
    }

    public int getPayingPrice() {
        return payingPrice;
    }

    public void setPayingPrice(int payingPrice) {
        this.payingPrice = payingPrice;
    }

    public String getCreativeId() {
        return creativeId;
    }

    public void setCreativeId(String creativeId) {
        this.creativeId = creativeId;
    }

    public int getBiddingPrice() {
        return biddingPrice;
    }

    public void setBiddingPrice(int biddingPrice) {
        this.biddingPrice = biddingPrice;
    }

    public String getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(String advertiserId) {
        this.advertiserId = advertiserId;
    }

    public String getUserTags() {
        return userTags;
    }

    public void setUserTags(String userTags) {
        this.userTags = userTags;
    }

    public int getStreamId() {
        return streamId;
    }

    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    public int getMlResult() {
        return mlResult;
    }

    public void setMlResult(int mlResult) {
        this.mlResult = mlResult;
    }
}