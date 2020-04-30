# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOINSTALL=$(GOCMD) install
BIN_DIR=bin


all: test build
build: 
		$(GOBUILD) -v ./...
install:
		$(GOINSTALL) ./...
test: 
		$(GOTEST) -v ./...
clean: 
		$(GOCLEAN)
