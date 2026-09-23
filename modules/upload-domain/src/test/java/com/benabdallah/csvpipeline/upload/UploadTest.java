package com.benabdallah.csvpipeline.upload;

import static org.assertj.core.api.Assertions.*;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UploadTest {
  @Test void followsHappyPath() {
    var upload = Upload.initiate(new UploadId(UUID.randomUUID()), "people.csv", "text/csv", 10, "abc", "key");
    upload.markReady(Instant.parse("2026-09-23T10:00:00Z"));
    upload.claimProcessing(Instant.parse("2026-09-23T10:01:00Z"));
    upload.succeed("external-42", Instant.parse("2026-09-23T10:02:00Z"));
    assertThat(upload.status()).isEqualTo(UploadStatus.SUCCEEDED);
    assertThat(upload.thirdPartyReference()).isEqualTo("external-42");
  }

  @Test void readyIsIdempotentButIllegalTransitionsAreRejected() {
    var upload = Upload.initiate(new UploadId(UUID.randomUUID()), "people.csv", "text/csv", 10, "abc", "key");
    upload.markReady(Instant.EPOCH);
    upload.markReady(Instant.EPOCH);
    assertThatThrownBy(() -> upload.succeed("ref", Instant.EPOCH)).isInstanceOf(IllegalStateException.class);
  }
}
