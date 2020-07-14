# power shell script

$sha1 = (git rev-parse HEAD).Trim()
$gitver = (git describe --tag).Trim()
$now = Get-Date -UFormat "%Y-%m-%d_%T"
echo "Building go binary with GitInfo $gitver $now $sha1"
go build -ldflags "-X github.com/synerex/synerex_sxutil.Sha1Ver=$sha1 -X github.com/synerex/synerex_sxutil.BuildTime=$now -X github.com/synerex/synerex_sxutil.GitVer=$gitver"
