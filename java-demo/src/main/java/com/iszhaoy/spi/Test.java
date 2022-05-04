package com.iszhaoy.spi;

import java.util.ServiceLoader;

public class Test {
    public static void main(String[] args) {
        ServiceLoader<UploadCDN> uploadCDNS = ServiceLoader.load(UploadCDN.class);
        for (UploadCDN uploadCDN : uploadCDNS) {
            uploadCDN.upload("file Path");
        }
    }B
}
