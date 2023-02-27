all: build

prepare:
	mkdir -p bin/

build-bench: prepare
	go build -o bin/infinibench .

build-playback: prepare
	go build -o bin/playback ./simulator/playback/

build: build-bench build-playback

simulate: build
	bin/playback -dryrun -lean simulator/samples/dal09_blobs_sample.csv