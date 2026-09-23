package com.benabdallah.csvpipeline.application.port.out; import com.benabdallah.csvpipeline.upload.UploadId;
public interface OutboxRepository { void appendUploadReady(UploadId uploadId); }
