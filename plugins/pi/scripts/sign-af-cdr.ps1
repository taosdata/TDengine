New-SelfSignedCertificate -DnsName tdengine@tdengine.int -Type CodeSigning -CertStoreLocation cert:\CurrentUser\My
Export-Certificate -Cert (Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert)[0] -FilePath code_signing.crt
Import-Certificate -FilePath .\code_signing.crt -Cert Cert:\CurrentUser\TrustedPublisher
Import-Certificate -FilePath .\code_signing.crt -Cert Cert:\CurrentUser\Root