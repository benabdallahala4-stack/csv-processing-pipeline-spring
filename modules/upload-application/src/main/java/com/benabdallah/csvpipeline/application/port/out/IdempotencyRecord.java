package com.benabdallah.csvpipeline.application.port.out; import com.benabdallah.csvpipeline.upload.UploadId;
public record IdempotencyRecord(String operation,String key,String requestHash,UploadId uploadId) {}
