# Security policy

Please report vulnerabilities privately through GitHub's **Report a vulnerability**
feature. Do not include production DSNs, authentication tokens, or raw logs in a
public issue.

The bridge deliberately:

- requires authentication on both ingest endpoints;
- redacts common token, password, DSN, and Authorization shapes;
- limits decompressed request and event sizes;
- runs as an unprivileged user in its container image;
- never logs request headers, query strings, DSNs, or authentication tokens.

Redaction is defense in depth, not a substitute for keeping secrets out of
application logs.
