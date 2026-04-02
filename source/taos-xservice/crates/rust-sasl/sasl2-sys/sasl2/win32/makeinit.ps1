$mechanism = @("anonymous", "crammd5", "digestmd5", "scram", "gssapiv2", "kerberos4", "login", "ntlm", "otp", "passdss", "plain", "srp", "gs2")
$pluginsDir = "..\plugins\"

for ($i = 0; $i -le $mechanism.Count - 1; $i++)
{
	$targetFilename = "$pluginsDir$($mechanism[$i])_init.c"
	if (-Not (Test-Path -Path $targetFilename) -Or (Get-ChildItem $targetFilename).CreationTime -lt (Get-ChildItem "init_mechanism.c").CreationTime) {
		(gc init_mechanism.c) -replace 'MECHANISM', $mechanism[$i] | Set-Content $targetFilename
		Write-Host " * Make init for  '" $mechanism[$i] "'"
	}
}

$auxprop = @("sasldb", "sql", "ldapdb")
for ($i = 0; $i -le $auxprop.Count - 1; $i++)
{
	$targetFilename = "$pluginsDir$($auxprop[$i])_init.c"
	if (-Not (Test-Path -Path $targetFilename) -Or (Get-ChildItem $targetFilename).CreationTime -lt (Get-ChildItem "init_auxprop.c").CreationTime) {
		(gc init_auxprop.c) -replace 'AUXPROP_REPLACE', $auxprop[$i] | Set-Content $targetFilename
		Write-Host " * Make init for  '" $auxprop[$i] "'"
	}
}

"SASL_CANONUSER_PLUG_INIT( ldapdb )" | Add-Content "$($pluginsDir)ldapdb_init.c"