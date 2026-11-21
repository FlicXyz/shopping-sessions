# Shopping Sessions: native C++ pipeline.
# The build stage compiles two static binaries from the same source:
#   /app/pipeline     -march=x86-64-v3 (AVX2/BMI2), runs on any Cascade Lake or newer
#   /app/pipeline-v4  -march=x86-64-v4 (AVX-512)
# /app/pipeline checks the CPU at start and execs pipeline-v4 only when every
# AVX-512 feature v4 needs is present, so a CPU without them never sees an
# AVX-512 instruction.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ /src/
ARG CXXFLAGS="-O3 -std=c++17 -static -pthread -fno-stack-protector"
RUN g++ $CXXFLAGS -march=x86-64-v4 -o /pipeline-v4 main.cpp && \
    g++ $CXXFLAGS -march=x86-64-v3 -DSS_DISPATCH_TO='"/app/pipeline-v4"' -o /pipeline main.cpp

FROM debian:bookworm-slim
COPY --from=build /pipeline /pipeline-v4 /app/
WORKDIR /app
CMD ["/app/pipeline"]
