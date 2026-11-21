# Shopping Sessions: native C++ pipeline.
# Build stage compiles one static binary; the run stage only holds that binary.
# -march=x86-64-v3 (AVX2/BMI2) runs on every Cascade Lake or newer Xeon scorer,
# whatever CPU the image is built on.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ /src/
RUN g++ -O3 -std=c++17 -march=x86-64-v3 -static -pthread -o /pipeline main.cpp

FROM debian:bookworm-slim
COPY --from=build /pipeline /app/pipeline
WORKDIR /app
CMD ["/app/pipeline"]
