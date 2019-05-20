#!/bin/bash

protoc3 --proto_path=src/main/resources --java_out=src/main/java/ src/main/resources/commands.proto