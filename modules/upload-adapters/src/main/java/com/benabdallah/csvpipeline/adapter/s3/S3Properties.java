package com.benabdallah.csvpipeline.adapter.s3; import java.time.Duration; public record S3Properties(String bucket,Duration presignTtl) {}
