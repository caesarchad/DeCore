Docker image containing rust nightly and some preinstalled crates used in CI.

This image may be manually updated by running `CI=true ./build.sh` if you are a member
of the [Morgan Labs](https://hub.docker.com/u/morganlabs/) Docker Hub
organization, but it is also automatically updated periodically by
[this automation](https://buildkite.com/morgan-labs/morgan-ci-docker-rust-nightly).

## Moving to a newer nightly

We pin the version of nightly (see the `ARG nightly=xyz` line in `Dockerfile`)
to avoid the build breaking at unexpected times, as occasionally nightly will
introduce breaking changes.

To update the pinned version:
1. Run `ci/docker-rust-nightly/build.sh` to rebuild the nightly image locally,
   or potentially `ci/docker-rust-nightly/build.sh YYYY-MM-DD` if there's a
   specific YYYY-MM-DD that is desired (default is today's build).
1. Run `SOLANA_DOCKER_RUN_NOSETUID=1 ci/docker-run.sh --nopull morganlabs/rust-nightly:YYYY-MM-DD ci/test-coverage.sh`
   to confirm the new nightly image builds.  Fix any issues as needed
1. Run `docker login` to enable pushing images to Docker Hub, if you're authorized.
1. Run `CI=true ci/docker-rust-nightly/build.sh YYYY-MM-DD` to push the new nightly image to dockerhub.com.
1. Modify the `morganlabs/rust-nightly:YYYY-MM-DD` reference in `ci/rust-version.sh` from the previous to
   new *YYYY-MM-DD* value, send a PR with this change and any codebase adjustments needed.

## Troubleshooting

### Resource is denied

When running `CI=true ci/docker-rust-nightly/build.sh`, you see:

```
denied: requested access to the resource is denied
```

Run `docker login` to enable pushing images to Docker Hub. Contact @mvines or @garious
to get write access.
