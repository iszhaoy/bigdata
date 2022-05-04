package com.iszhaoy.spi;

public class QiyiCDN implements UploadCDN {

    @Override
    public void upload(String url) {
        System.out.println("upload to qiyi cdn");
    }
}
