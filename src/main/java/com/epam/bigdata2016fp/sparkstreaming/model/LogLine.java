package com.epam.bigdata2016fp.sparkstreaming.model;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class LogLine implements Serializable {

    private String bidId;
    private static final byte[] bidIdBytes = Bytes.toBytes("BID_ID");
    private String timestamp;
    private static final byte[] timestampBytes = Bytes.toBytes("TIMESTAMP_DATE");
    private String iPinyouId;
    private static final byte[] iPinyouIdBytes = Bytes.toBytes("IPINYOU_ID");
    private String userAgent;
    private static final byte[] userAgentBytes = Bytes.toBytes("USER_AGENT");
    private String ip;
    private static final byte[] ipBytes = Bytes.toBytes("IP");
    private int region;
    private static final byte[] regionBytes = Bytes.toBytes("REGION");
    private int city;
    private static final byte[] cityBytes = Bytes.toBytes("CITY");
    private int addExchange;
    private static final byte[] addExchangeBytes = Bytes.toBytes("AD_EXCHANGE");
    private String domain;
    private static final byte[] domainBytes = Bytes.toBytes("DOMAIN");
    private String url;
    private static final byte[] urlBytes = Bytes.toBytes("URL");
    private String anonUrl;
    private static final byte[] anonUrlBytes = Bytes.toBytes("ANONYMOUS_URL_ID");
    private String addSlot;
    private static final byte[] addSlotBytes = Bytes.toBytes("AD_SLOT_ID");
    private int addSlotWidth;
    private static final byte[] addSlotWidthBytes = Bytes.toBytes("AD_SLOT_WIDTH");
    private int addSlotHeight;
    private static final byte[] addSlotHeightBytes = Bytes.toBytes("AD_SLOT_HEIGHT");
    private int addSlotVisability;
    private static final byte[] addSlotVisabilityBytes = Bytes.toBytes("AD_SLOT_VISIBILITY");
    private int addSlotFormat;
    private static final byte[] addSlotFormatBytes = Bytes.toBytes("AD_SLOT_FORMAT");
    private int payingPrice;
    private static final byte[] payingPriceBytes = Bytes.toBytes("PAYING_PRICE");
    private String creativeId;
    private static final byte[] creativeIdBytes = Bytes.toBytes("CREATIVE_ID");
    private int biddingPrice;
    private static final byte[] biddingPriceBytes = Bytes.toBytes("BIDDING_PRICE");
    private String addvertiseId;
    private static final byte[] addvertiseIdBytes = Bytes.toBytes("ADVERTISER_ID");
    private String userTags;
    private static final byte[] userTagsBytes = Bytes.toBytes("USER_TAGS");
    private int streamId;
    private static final byte[] streamIdBytes = Bytes.toBytes("STREAM_ID");
    private String tagsList;
    public static final byte[] tagsListBytes = Bytes.toBytes("TAGS_LIST");
    private CityInfo geoPoint;
    public static final byte[] latBytes = Bytes.toBytes("LAT");
    public static final byte[] lonBytes = Bytes.toBytes("LON");


    public static Put convertToPut(LogLine line, String columnFamily) {
        byte[] callFamilyBytes = Bytes.toBytes("DATA");
        String rowKey = line.iPinyouId + "_" + line.timestamp;
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(callFamilyBytes, bidIdBytes, Bytes.toBytes(line.bidId));
        put.addColumn(callFamilyBytes, timestampBytes, Bytes.toBytes(line.timestamp));
        put.addColumn(callFamilyBytes, iPinyouIdBytes, Bytes.toBytes(line.iPinyouId));
        put.addColumn(callFamilyBytes, userAgentBytes, Bytes.toBytes(line.userAgent));
        put.addColumn(callFamilyBytes, ipBytes, Bytes.toBytes(line.ip));
        put.addColumn(callFamilyBytes, cityBytes, Bytes.toBytes(line.city));
        put.addColumn(callFamilyBytes, regionBytes, Bytes.toBytes(line.region));
        put.addColumn(callFamilyBytes, addExchangeBytes, Bytes.toBytes(line.addExchange));
        put.addColumn(callFamilyBytes, domainBytes, Bytes.toBytes(line.domain));
        put.addColumn(callFamilyBytes, urlBytes, Bytes.toBytes(line.url));
        put.addColumn(callFamilyBytes, anonUrlBytes, Bytes.toBytes(line.anonUrl));
        put.addColumn(callFamilyBytes, addSlotBytes, Bytes.toBytes(line.addSlot));
        put.addColumn(callFamilyBytes, addSlotWidthBytes, Bytes.toBytes(line.addSlotWidth));
        put.addColumn(callFamilyBytes, addSlotHeightBytes, Bytes.toBytes(line.addSlotHeight));
        put.addColumn(callFamilyBytes, addSlotVisabilityBytes, Bytes.toBytes(line.addSlotVisability));
        put.addColumn(callFamilyBytes, addSlotFormatBytes, Bytes.toBytes(line.addSlotFormat));
        put.addColumn(callFamilyBytes, payingPriceBytes, Bytes.toBytes(line.payingPrice));
        put.addColumn(callFamilyBytes, creativeIdBytes, Bytes.toBytes(line.creativeId));
        put.addColumn(callFamilyBytes, biddingPriceBytes, Bytes.toBytes(line.biddingPrice));
        put.addColumn(callFamilyBytes, addvertiseIdBytes, Bytes.toBytes(line.addvertiseId));
        put.addColumn(callFamilyBytes, userTagsBytes, Bytes.toBytes(line.userTags));
        put.addColumn(callFamilyBytes, streamIdBytes, Bytes.toBytes(line.streamId));

        put.addColumn(callFamilyBytes, tagsListBytes, Bytes.toBytes(line.tagsList));
        put.addColumn(callFamilyBytes, latBytes, Bytes.toBytes(String.valueOf(line.geoPoint.getLat())));
        put.addColumn(callFamilyBytes, lonBytes, Bytes.toBytes(String.valueOf(line.geoPoint.getLon())));

        /*put.addColumn(callFamilyBytes, latBytes, ByteBuffer.allocate(4).putFloat((line.geoPoint != null) ? line.geoPoint.getLat() : 0).array());
        put.addColumn(callFamilyBytes, lonBytes, ByteBuffer.allocate(4).putFloat((line.geoPoint != null) ? line.geoPoint.getLon() : 0).array());*/
        return put;
    }

    public static LogLine parseLogLine(String line) {
        String[] arr = line.split("\\t");
        LogLine logLine = new LogLine();
        logLine.bidId = arr[0];
        logLine.timestamp = arr[1];
        logLine.iPinyouId = arr[2];
        logLine.userAgent = arr[3];
        logLine.ip = arr[4];
        logLine.region = Integer.parseInt(arr[5]);
        logLine.city = Integer.parseInt(arr[6]);
        logLine.addExchange = Integer.parseInt(arr[7]);
        logLine.domain = arr[8];
        logLine.url = arr[9];
        logLine.anonUrl = arr[10];
        logLine.addSlot = arr[11];
        logLine.addSlotWidth = Integer.parseInt(arr[12]);
        logLine.addSlotHeight = Integer.parseInt(arr[13]);
        logLine.addSlotVisability = Integer.parseInt(arr[14]);
        logLine.addSlotFormat = Integer.parseInt(arr[15]);
        logLine.payingPrice = Integer.parseInt(arr[16]);
        logLine.creativeId = arr[17];
        logLine.biddingPrice = Integer.parseInt(arr[18]);
        logLine.addvertiseId = arr[19];
        logLine.userTags = arr[20];
        logLine.streamId = Integer.parseInt(arr[21]);
        return logLine;
    }

    @Override
    public String toString() {
        return "LogLine{" +
                "bidId='" + bidId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", iPinyouId='" + iPinyouId + '\'' +
                ", userAgent='" + userAgent + '\'' +
                ", ip='" + ip + '\'' +
                ", region=" + region +
                ", city=" + city +
                ", addExchange=" + addExchange +
                ", domain='" + domain + '\'' +
                ", url='" + url + '\'' +
                ", anonUrl='" + anonUrl + '\'' +
                ", addSlot='" + addSlot + '\'' +
                ", addSlotWidth=" + addSlotWidth +
                ", addSlotHeight=" + addSlotHeight +
                ", addSlotVisability=" + addSlotVisability +
                ", addSlotFormat=" + addSlotFormat +
                ", payingPrice=" + payingPrice +
                ", creativeId='" + creativeId + '\'' +
                ", biddingPrice=" + biddingPrice +
                ", addvertiseId='" + addvertiseId + '\'' +
                ", userTags='" + userTags + '\'' +
                ", streamId=" + streamId +
                '}';
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public String getUserTags() {
        return userTags;
    }

    public void setUserTags(String userTags) {
        this.userTags = userTags;
    }

    public String getTagsList() {
        return tagsList;
    }

    public void setTagsList(String tagsList) {
        this.tagsList = tagsList;
    }

    public CityInfo getGeoPoint() {
        return geoPoint;
    }

    public void setGeoPoint(CityInfo geoPoint) {
        this.geoPoint = geoPoint;
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(String iPinyouId) {
        this.iPinyouId = iPinyouId;
    }
}
