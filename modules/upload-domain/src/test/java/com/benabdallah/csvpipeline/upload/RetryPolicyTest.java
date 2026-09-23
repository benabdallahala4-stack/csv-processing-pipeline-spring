package com.benabdallah.csvpipeline.upload;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {
  @Test void growsDelayAndCapsIt() {
    var policy = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMinutes(5));
    var seed = UUID.fromString("00000000-0000-0000-0000-000000000001");
    assertThat(policy.nextDelay(2, seed)).isGreaterThan(policy.nextDelay(1, seed));
    assertThat(policy.nextDelay(20, seed)).isLessThanOrEqualTo(Duration.ofMinutes(5));
  }
}
