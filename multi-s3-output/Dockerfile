FROM registry.access.redhat.com/ubi8/go-toolset:latest AS builder

USER root

COPY . /opt/output
WORKDIR /opt/output

RUN go build -buildmode=c-shared -o out_multi_s3.so .


FROM fluent/fluent-bit:1.6

WORKDIR /opt/fluent-bit/

COPY --from=builder /opt/output/out_multi_s3.so /opt/fluent-bit/out_multi_s3.so

ENTRYPOINT ["/fluent-bit/bin/fluent-bit", "-e", "/opt/fluent-bit/out_multi_s3.so", "-c", "/etc/fluent/config.d/fluent-bit.conf"]

