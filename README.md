# Fluent-Bit `multi-s3` Output Plugin

Output plugin (Go) for fluent bit with support for multiple S3 instances

## Installation

Build the plugin (shared) library:

```shell script
cd multi-s3-output && go build -buildmode=c-shared -o out_multi_s3.so .
``` 

Run `fluent-bit` with the new plugin:

```shell script
fluent-bit -e out_multi_s3.so -c fluent-bit.conf
``` 
