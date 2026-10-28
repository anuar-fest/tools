# All generation runs through the buf version pinned in go.mod (`go tool buf`).
.PHONY: generate lint breaking check tidy

generate: ## regenerate gen/go from proto/ (cleans stale output first)
	go tool buf generate

lint: ## style and naming rules
	go tool buf lint

breaking: ## fail on wire-incompatible changes against main
	go tool buf breaking --against '.git#branch=main'

check: generate ## what CI runs: regenerate and fail on any diff
	@git diff --exit-code -- gen/ || (echo "generated code is stale: run 'make generate' and commit"; exit 1)
	go build ./...
	go vet ./...

tidy:
	go mod tidy
