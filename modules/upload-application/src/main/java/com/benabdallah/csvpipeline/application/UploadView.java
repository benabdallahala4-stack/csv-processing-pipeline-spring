package com.benabdallah.csvpipeline.application;
import com.benabdallah.csvpipeline.upload.*;
public record UploadView(UploadId id,String fileName,UploadStatus status,int attemptCount,String thirdPartyReference,String failureCode) { public static UploadView from(Upload u){return new UploadView(u.id(),u.fileName(),u.status(),u.attemptCount(),u.thirdPartyReference(),u.failureCode());} }
