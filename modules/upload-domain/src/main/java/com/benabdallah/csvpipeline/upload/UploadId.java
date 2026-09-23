package com.benabdallah.csvpipeline.upload;
import java.util.Objects; import java.util.UUID;
public record UploadId(UUID value) { public UploadId { Objects.requireNonNull(value); } public static UploadId newId(){ return new UploadId(UUID.randomUUID()); } @Override public String toString(){ return value.toString(); } }
