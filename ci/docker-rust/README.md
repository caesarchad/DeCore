Docker image containing rust and some preinstalled packages used in CI.

This image manually maintained:
1. Edit `Dockerfile` to match the desired rust version
2. Run `./build.sh` to publish the new image, if you are a member of the [Morgan
   Labs](https://hub.docker.com/u/morganlabs/) Docker Hub organization.

