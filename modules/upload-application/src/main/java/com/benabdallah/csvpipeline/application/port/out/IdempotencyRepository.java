package com.benabdallah.csvpipeline.application.port.out; import java.util.Optional;
public interface IdempotencyRepository { Optional<IdempotencyRecord> find(String operation,String key); void save(IdempotencyRecord record); }
