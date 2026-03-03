# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred): Use the [Security Advisory](https://github.com/NP-compete/langgraph-checkpoint-snowflake/security/advisories/new) feature to privately report the vulnerability.

2. **Email**: Send details to the maintainers (include "SECURITY" in the subject line).

### What to Include

Please include the following information in your report:

- Type of vulnerability (e.g., SQL injection, credential exposure, etc.)
- Full paths of source file(s) related to the vulnerability
- Location of the affected source code (tag/branch/commit or direct URL)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact assessment of the vulnerability

### Response Timeline

- **Initial Response**: Within 48 hours of submission
- **Status Update**: Within 7 days with an assessment
- **Resolution Target**: Security patches within 30 days for critical issues

### What to Expect

1. **Acknowledgment**: We will acknowledge receipt of your report
2. **Assessment**: We will investigate and assess the severity
3. **Communication**: We will keep you informed of our progress
4. **Credit**: We will credit you in the security advisory (unless you prefer anonymity)
5. **Disclosure**: We coordinate disclosure timing with you

## Security Best Practices

When using this library, follow these security recommendations:

### Authentication

- **Use key pair authentication** instead of password authentication in production
- Store private keys securely with appropriate file permissions (600)
- Use encrypted private keys with strong passphrases
- Rotate keys periodically

```bash
# Generate encrypted private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -v2 aes256

# Set secure permissions
chmod 600 rsa_key.p8
```

### Environment Variables

- Never commit credentials to version control
- Use secret management solutions (HashiCorp Vault, AWS Secrets Manager, etc.)
- Set restrictive permissions on `.env` files

### Snowflake Configuration

- Use dedicated service accounts with minimal required permissions
- Enable Snowflake's audit logging
- Configure network policies to restrict access
- Use separate warehouses for different environments

### Redis Cache (if enabled)

- Use TLS for Redis connections in production
- Enable Redis authentication (AUTH)
- Use private networks for Redis instances
- Consider Redis ACLs for fine-grained access control

## Known Security Considerations

### Data at Rest

- Checkpoint data is stored in Snowflake tables
- Snowflake provides encryption at rest by default
- Consider additional encryption for sensitive channel values

### Data in Transit

- Snowflake connector uses TLS by default
- Redis connections should use TLS in production

### Credential Handling

- Credentials are never logged by this library
- Connection objects should not be serialized
- Use context managers to ensure proper cleanup

## Security Updates

Security updates are released as patch versions. We recommend:

1. Enabling Dependabot alerts for this repository
2. Subscribing to release notifications
3. Regularly updating to the latest patch version

```bash
pip install --upgrade langgraph-checkpoint-snowflake
```
