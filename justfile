set shell := ["bash", "-euo", "pipefail", "-c"]


# Run all project checks.
check: tidy typos fmt lint vet test fuzz

# Update the Go module files.
tidy:
  go mod tidy

# Find spelling errors.
typos:
  typos

# Format all tracked source files.
fmt: markdown-fmt
  golangci-lint fmt ./...
  git ls-files -z -- '*.go' | xargs -0 -r gofmt -w

# Format all tracked Markdown files.
markdown-fmt:
  git ls-files -z -- '*.md' | xargs -0 -r mdformat --wrap 80

# Run the Go linters.
lint:
  golangci-lint run ./...

# Find suspicious Go constructs.
vet:
  go vet ./...

# Run all tests with the race detector and coverage measurement.
test:
  go test -race -cover ./...

# Run each Go fuzz target. The duration applies to each target.
fuzz fuzztime=env_var_or_default('FUZZ_TIME', '5s') fuzzparallel=env_var_or_default('FUZZ_PARALLEL', '2'):
  @found=0; \
  while IFS=: read -r file _ target; do \
    found=1; \
    package="./$(dirname "$file")"; \
    printf 'Fuzzing %s in %s for %s\n' "$target" "$package" {{quote(fuzztime)}}; \
    go test "$package" -run '^$' -fuzz "^${target}$" -fuzztime {{quote(fuzztime)}} -parallel {{quote(fuzzparallel)}}; \
  done < <(rg --line-number --no-heading --replace '$1' '^func (Fuzz[[:alnum:]_]+)\(.*$' --glob '*_test.go'); \
  if ((found == 0)); then printf 'No Go fuzz targets found.\n'; fi

# Build all packages.
build:
  go build ./...

# Show the available recipes.
default:
  @just --list
