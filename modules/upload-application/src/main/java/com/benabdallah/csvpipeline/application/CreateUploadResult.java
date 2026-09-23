package com.benabdallah.csvpipeline.application;
import com.benabdallah.csvpipeline.upload.*; import java.net.URI; import java.time.Instant; import java.util.Map;
public record CreateUploadResult(UploadId uploadId,UploadStatus status,URI uploadUrl,Instant expiresAt,Map<String,String> requiredHeaders) {}
