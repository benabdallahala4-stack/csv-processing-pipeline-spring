package com.benabdallah.csvpipeline.api;
import com.benabdallah.csvpipeline.application.*; import com.benabdallah.csvpipeline.application.service.*; import com.benabdallah.csvpipeline.upload.UploadId; import jakarta.validation.Valid; import jakarta.validation.constraints.*; import java.net.URI; import java.time.Instant; import java.util.*; import org.springframework.http.*; import org.springframework.web.bind.annotation.*;
@RestController @RequestMapping("/api/v1/uploads") public class UploadController {
 private final CreateUploadService create;private final CompleteUploadService complete;public UploadController(CreateUploadService c,CompleteUploadService p){create=c;complete=p;}
 @PostMapping public ResponseEntity<CreateResponse> create(@RequestHeader("Idempotency-Key")String key,@Valid @RequestBody CreateRequest body){var r=create.create(key,new CreateUploadCommand(body.fileName(),body.contentType(),body.sizeBytes(),body.sha256()));return ResponseEntity.created(URI.create("/api/v1/uploads/"+r.uploadId())).body(new CreateResponse(r.uploadId().value(),r.status().name(),r.uploadUrl(),r.expiresAt(),r.requiredHeaders()));}
 @PostMapping("/{id}/complete") public UploadView complete(@PathVariable UUID id){return complete.complete(new UploadId(id));} @GetMapping("/{id}") public UploadView get(@PathVariable UUID id){return complete.get(new UploadId(id));}
 public record CreateRequest(@NotBlank String fileName,@Pattern(regexp="text/csv|application/csv")String contentType,@Positive @Max(104857600)long sizeBytes,@Pattern(regexp="[a-fA-F0-9]{64}")String sha256){}
 public record CreateResponse(UUID uploadId,String status,URI uploadUrl,Instant expiresAt,Map<String,String> requiredHeaders){}
}
