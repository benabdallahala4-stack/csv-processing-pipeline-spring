package com.benabdallah.csvpipeline.upload;
import java.time.Duration; import java.util.UUID;
public record RetryPolicy(Duration base, Duration maximum) {
  public RetryPolicy { if(base.isNegative()||base.isZero()||maximum.compareTo(base)<0) throw new IllegalArgumentException("invalid retry durations"); }
  public Duration nextDelay(int attempt, UUID seed){ if(attempt<1) throw new IllegalArgumentException("attempt must be positive"); long multiplier=1L<<Math.min(attempt-1,30); long raw=Math.min(maximum.toMillis(),Math.multiplyExact(base.toMillis(),multiplier)); double jitter=.8+(Math.floorMod(seed.hashCode(),401)/1000.0); return Duration.ofMillis(Math.min(maximum.toMillis(),Math.max(base.toMillis(),(long)(raw*jitter)))); }
}
