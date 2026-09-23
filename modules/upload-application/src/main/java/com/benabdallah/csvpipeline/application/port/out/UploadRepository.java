package com.benabdallah.csvpipeline.application.port.out; import com.benabdallah.csvpipeline.upload.*; import java.util.Optional;
public interface UploadRepository { void save(Upload upload); Optional<Upload> find(UploadId id); Optional<Upload> findForUpdate(UploadId id); void update(Upload upload); }
