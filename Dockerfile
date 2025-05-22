FROM rust:1.87 AS build
WORKDIR /build/vector-store-text-build
RUN --mount=type=bind,target=.,rw cargo b -r && cp target/release/vector-store-text ..

FROM ubuntu:24.04
RUN mkdir /opt/vector-store-text
COPY --from=build /build/vector-store-text /opt/vector-store-text
CMD /opt/vector-store-text/vector-store-text
