The `java-v8.properties` and `java-v9.properties` fixtures were generated with
OpenJDK 17 `java.util.Properties.store(OutputStream)`, including its generated
comments. Their byte contents are retained verbatim.

`java-byte-stream.properties` supplies raw ISO-8859-1, UTF-8-looking byte pairs,
continuation, escaped separators, and UTF-16 surrogate-pair cases. Java 17
`Properties.load(InputStream)` independently loaded all three files and verified
the exact values asserted in `table_properties_compatibility_tests.rs`.

The Rust tests do not require a Java runtime.
