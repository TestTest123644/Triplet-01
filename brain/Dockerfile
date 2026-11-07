# The Brain, with its mocks and its Test Book, so a container can run standalone or join the
# triplet network. One stage: the image is small enough and a build stage would only hide
# which versions are actually installed.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src:/app

WORKDIR /app

# librdkafka is what confluent-kafka binds to; the wheels bundle it, but the build tools are
# needed if pip has to fall back to a source build on this platform.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir .

COPY policy/ policy/
COPY params/ params/
COPY profiles/ profiles/
COPY schemas/ schemas/
COPY config/ config/
COPY contracts/ contracts/
COPY scenarios/ scenarios/
COPY mocks/ mocks/
COPY timeline/ timeline/
COPY conformance/ conformance/
COPY Makefile INTERFACE.md ./

RUN useradd --create-home --uid 10001 brain && mkdir -p /app/logs && chown -R brain /app
USER brain

EXPOSE 8100 8110 8120

# The query API answers before the Brain has a verdict to give, so this is a fair check.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8100/health', timeout=2).status == 200 else 1)"

CMD ["python", "-m", "city_brain", "--config", "config/triplet.yaml"]
