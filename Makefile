all:build-image

build-image:
	podman manifest rm cityworks-puller:latest
	podman build --jobs=2 --platform=linux/amd64 --manifest cityworks-puller:latest .