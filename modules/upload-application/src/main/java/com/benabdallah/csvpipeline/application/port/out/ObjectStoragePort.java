package com.benabdallah.csvpipeline.application.port.out; import com.benabdallah.csvpipeline.upload.Upload; import java.io.InputStream;
public interface ObjectStoragePort { PresignedUpload presignPut(Upload upload); ObjectMetadata head(Upload upload); InputStream openStream(Upload upload); }
