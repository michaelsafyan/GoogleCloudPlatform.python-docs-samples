#! /usr/bin/env bash

function BuildAndRun() {
    docker build -t gcp-python-openllmetry-example
    docker run -it gcp-python-openllmetry-example
}

BuildAndRun
