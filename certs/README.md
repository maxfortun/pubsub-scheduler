# Custom Certificates

Place custom CA certificates here for corporate proxies (Zscaler, etc.).

## Usage

1. Export your corporate CA certificate(s) as PEM files (`.crt` or `.pem`)
2. Place them in this directory
3. Build with: `docker build -t pubsub-scheduler .`

The Dockerfile will automatically detect and install any `.crt` or `.pem` files found here.

## Example: Exporting Zscaler Certificate

### macOS
```bash
security find-certificate -a -p /Library/Keychains/System.keychain | \
  awk '/Zscaler/,/END CERTIFICATE/' > certs/zscaler.crt
```

### Windows
```powershell
# Export from Certificate Manager (certmgr.msc)
# Trusted Root Certification Authorities > Certificates > Zscaler Root CA
# Right-click > All Tasks > Export > Base-64 encoded X.509 (.CER)
```

### Linux
```bash
# Usually in /etc/ssl/certs/ or /usr/local/share/ca-certificates/
cp /path/to/zscaler.crt certs/
```

## Files in this directory

- `*.crt`, `*.pem` — Certificate files (gitignored)
- `README.md` — This file (tracked)
