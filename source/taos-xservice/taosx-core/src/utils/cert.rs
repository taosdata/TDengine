use tonic::transport::Certificate;

pub fn parse_certificate(cert: &str) -> Result<Certificate, std::io::Error> {
    let cert = cert.trim();
    if cert.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Certificate is empty",
        ));
    }
    if cert.starts_with("-----BEGIN") {
        return Ok(Certificate::from_pem(cert));
    }
    let content = std::fs::read(cert)?;
    let cert = Certificate::from_pem(content);
    Ok(cert)
}
pub fn parse_certificate_to_string(cert: &str) -> Result<String, std::io::Error> {
    let cert = cert.trim();
    if cert.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Certificate is empty",
        ));
    }
    if cert.starts_with("-----BEGIN") {
        return Ok(cert.to_string());
    }
    let content = std::fs::read_to_string(cert)?;
    Ok(content)
}
