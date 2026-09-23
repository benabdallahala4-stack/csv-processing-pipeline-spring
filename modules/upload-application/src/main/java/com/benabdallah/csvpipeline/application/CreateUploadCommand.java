package com.benabdallah.csvpipeline.application;
public record CreateUploadCommand(String fileName,String contentType,long sizeBytes,String sha256) {}
