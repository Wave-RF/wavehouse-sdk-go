module github.com/Wave-RF/wavehouse-sdk-go

// The floor matches grafana-plugin-sdk-go's and the oldest Go release still
// supported. The newest things this module uses are testing/synctest,
// errors.AsType and new(expr) (Go 1.26). Keep it a release floor, not a patch pin.
go 1.26

// Test-only: the SDK's own code imports nothing outside the standard library.
require (
	github.com/stretchr/testify v1.12.1
	go.uber.org/goleak v1.3.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect
