# ADR 002: Direct AWS S3 upload

**Status:** Accepted

The API returns a short-lived presigned PUT URL. File bytes travel from the client to AWS S3, avoiding API heap pressure and request timeouts. The API verifies size, content type, and checksum metadata before completion.
