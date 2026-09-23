package com.benabdallah.csvpipeline.application;
import static org.assertj.core.api.Assertions.assertThat;
import com.benabdallah.csvpipeline.application.port.out.*; import com.benabdallah.csvpipeline.application.service.CreateUploadService; import com.benabdallah.csvpipeline.upload.*;
import java.net.URI; import java.time.Instant; import java.util.*; import org.junit.jupiter.api.Test;
class CreateUploadServiceTest {
 @Test void replaysTheSameIdempotentRequest(){
  Map<String,IdempotencyRecord> keys=new HashMap<>(); Map<UploadId,Upload> uploads=new HashMap<>();
  UploadRepository repo=new UploadRepository(){public void save(Upload u){uploads.put(u.id(),u);}public Optional<Upload> find(UploadId id){return Optional.ofNullable(uploads.get(id));}public Optional<Upload> findForUpdate(UploadId id){return find(id);}public void update(Upload u){save(u);}};
  IdempotencyRepository idem=new IdempotencyRepository(){public Optional<IdempotencyRecord> find(String operation,String key){return Optional.ofNullable(keys.get(key));}public void save(IdempotencyRecord r){keys.put(r.key(),r);}};
  ObjectStoragePort s3=new ObjectStoragePort(){public PresignedUpload presignPut(Upload u){return new PresignedUpload(URI.create("https://s3.example/"+u.id()),Instant.MAX,Map.of());}public ObjectMetadata head(Upload u){throw new UnsupportedOperationException();}public java.io.InputStream openStream(Upload u){throw new UnsupportedOperationException();}};
  TransactionRunner tx=new TransactionRunner(){public <T>T required(java.util.function.Supplier<T> work){return work.get();}};
  var service=new CreateUploadService(repo,idem,s3,tx); var command=new CreateUploadCommand("people.csv","text/csv",12,"abc");
  assertThat(service.create("key-1",command).uploadId()).isEqualTo(service.create("key-1",command).uploadId()); assertThat(uploads).hasSize(1);
 }
}
