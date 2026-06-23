VERSION := $(shell git describe --tags)
COMMIT  := $(shell git log -1 --format='%H')

all: install

LD_FLAGS = -X github.com/binaryholdings/cosmos-pruner/cmd.Version=$(VERSION) \
	-X github.com/binaryholdings/cosmos-pruner/cmd.Commit=$(COMMIT) \

BUILD_FLAGS := -ldflags '$(LD_FLAGS)'

build:
	CGO_ENABLED=0 go build -tags pebbledb -mod readonly $(BUILD_FLAGS) -o build/cosmprund main.go

install:
	go install -tags pebbledb -mod readonly $(BUILD_FLAGS) ./...

# celestia-dedicated binary: pins celestia's iavl/store/log fork stack (go.celestia.mod)
# so SnapshotAndRestoreApp reproduces the correct app hash. The `celestia` build tag flips
# celestia/mocha-4 to SnapshotAndRestoreApp (cmd/chains_celestia.go). USE ONLY for
# celestia/mocha — never against other chains.
CELESTIA_BUILD_FLAGS := -tags 'pebbledb celestia' -modfile=go.celestia.mod -mod readonly $(BUILD_FLAGS)

build-celestia:
	CGO_ENABLED=0 go build $(CELESTIA_BUILD_FLAGS) -o build/cosmprund-celestia main.go

install-celestia:
	go install $(CELESTIA_BUILD_FLAGS) ./...

clean:
	rm -rf build

.PHONY: all lint test race msan tools clean build build-celestia install-celestia
