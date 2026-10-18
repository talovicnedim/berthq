# Test TLS files

`ca.pem`, `cert.pem` and `key.pem` are a throwaway certificate authority and a `localhost`
certificate signed by it, made only for the HTTP/2 tests in `tests/http2.rs`. They are valid
until 2126, so the tests don't break when they get old.

`key.pem` is a real private key, but it is public on purpose and protects nothing. Nothing
outside these tests trusts this CA. If a secret scanner flags it, mark it as a test fixture.
Never use these files for a real server.
