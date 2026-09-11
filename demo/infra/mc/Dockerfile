# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# mc comes from MinIO's own published image rather than from a direct download.
# https://dl.min.io/client/mc/release/linux-amd64/mc now answers 410 GONE -- the binary was
# removed upstream, so the previous `wget` could never succeed again and it failed the whole
# integration-test matrix, on this branch and identically on oss/main.
#
# quay.io is already a dependency of this pipeline (ci.yml's integration-tests job pulls
# quay.io/minio/minio before the compose stack starts), so this adds no new trust boundary, and
# it removes a plain binary fetch in favour of a content-addressable image. mc is a static Go
# binary, so it runs on the alpine base below unchanged.
FROM quay.io/minio/mc:latest AS mc

FROM alpine

RUN apk update && apk add --no-cache \
	ca-certificates bash unzip

COPY --from=mc /usr/bin/mc /usr/local/bin/mc
RUN chmod +x /usr/local/bin/mc && mc --version

WORKDIR /opt/data
