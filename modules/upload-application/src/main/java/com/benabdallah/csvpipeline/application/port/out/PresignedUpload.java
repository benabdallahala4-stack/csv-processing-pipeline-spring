package com.benabdallah.csvpipeline.application.port.out; import java.net.URI; import java.time.Instant; import java.util.Map;
public record PresignedUpload(URI uri,Instant expiresAt,Map<String,String> requiredHeaders) {}
