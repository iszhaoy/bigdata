package com.iszhaoy.spi;

public class ChinaNetCDN implements UploadCDN{
    @Override
    public void upload(String url) {
        System.out.println("upload to chinaNet cdn");
    }
}
