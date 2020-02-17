version=0.1.0
export GOPROXY=direct

.PHONY: all dependencies clean test

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  dependencies  - install dependencies"
	@echo "  build         - build the source code"
	@echo "  docs          - build the documentation"
	@echo "  clean         - clean the source directory"
	@echo "  lint          - lint the source code"
	@echo "  fmt           - format the source code"
	@echo "  test          - test the source code"

dependencies:
	@go get -u golang.org/x/tools
	@go get -u golang.org/x/lint/golint
	@go get -u golang.org/x/tools/cmd/godoc
	@go get -u github.com/unchartedsoftware/witch
	@go get -u github.com/golang/protobuf/protoc-gen-go
	@make testenv
	@go get -d -v ./...

fmt:
	@go fmt ./...

clean:
	@rm -rf ./bin
	@rm -rf ./internal/rpc/*.pb.go

lint:
	@echo "Running go vet"
	@go vet ./...
	@echo "Running golint"
	@go list ./... | grep -v /vendor/ | xargs -L1 golint --set_exit_status
	@echo "Ensuring that internal code isn't used in the implementation of datasources"
	@! grep -ir internal datasource | grep -v util.go
	@echo "Ensuring that internal code isn't used in the implementation of integration tests"
	@! grep -ir internal internal/test/integration | grep -v util.go

testenv:
	@echo "Downloading NYC Taxi test files..."
	@mkdir -p testenv
	@cd testenv && curl -s http://assets.oculusinfo.com/salt/sample-data/taxi_one_day.csv | tail -n +2 | split --additional-suffix .csv -l 60000
	@echo "Finished downloading NYC Taxi test files."
	@echo "Downloading EDSM test files..."
	@mkdir -p testenv
	@cd testenv && curl -s https://www.edsm.net/dump/systemsWithCoordinates7days.json | tail -n +2 | head -n -1 | sed 's/,$$//' | sed 's/^....//' | split --additional-suffix .jsonl -l 50000
	@echo "Finished downloading EDSM test files."

# Can use this for a much larger dataset
edsm_big_data:
	@echo "Downloading EDSM test files..."
	@mkdir -p testenv
	@cd testenv && curl -s https://www.edsm.net/dump/systemsWithCoordinates.json | tail -n +2 | head -n -1 | sed 's/,$$//' | sed 's/^....//' | split --additional-suffix .jsonl -l 50000
	@echo "Finished downloading EDSM test files."

test: build testenv
	@echo "Running tests..."
	@go test -short -count=1 ./...

testall: build testenv
	@echo "Running tests..."
	@go test -timeout 30m -count=1 ./...

testv: build testenv
	@echo "Running tests..."
	@go test -short -v ./...

testvall: build testenv
	@echo "Running tests..."
	@go test -timeout 30m -count=1 ./...

edsm: build testenv
	@echo "Running tests..."
	@go test -timeout 30m -count=1 -run TestEDSMHeatmap ./...

generate:
	@echo "Generating protobuf code..."
	@go generate ./...
	@echo "Finished generating protobuf code."

build: generate lint
	@echo "Building sif..."
	@go build ./...
	@go mod tidy
	@echo "Finished building sif."

docs:
	@echo "Serving docs on http://localhost:6060"
	@witch --cmd="godoc -http=localhost:6060" --watch="**/*.go" --ignore="vendor,.git,**/*.pb.go" --no-spinner

watch:
	@witch --cmd="make build" --watch="**/*.go" --ignore="vendor,.git,**/*.pb.go" --no-spinner
