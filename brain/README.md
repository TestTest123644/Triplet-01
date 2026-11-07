# City Brain

The decision layer of triplet 07. It reads the Digital Twin's sensor readings and the
Guardian's trust verdicts, works out what the city needs, and asks the Twin to do it —
refusing, holding or escalating when the evidence will not carry the action.

`INTERFACE.md` is the one-page contract. `docs/ARCHITECTURE.md` is the design.

## Quickstart

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

### Standalone: the Brain against its own mocks

No broker, no partners, no network. One scenario, one hash-chained log, one summary:

```bash
make scenario name=T02
```

The run writes `logs/T02-e0-<timestamp>.jsonl`, `…-summary.json` and `….pub` (the key its
records are signed with). Check the log end to end:

```bash
python -m city_brain.adapters.files.jsonl_log --verify logs/T02-e0-<timestamp>.jsonl --pubkey "$(cat logs/T02-e0-<timestamp>.pub)"
```

The whole Test Book (T01–T12, spec §16):

```bash
make book
```

### The Timeline

Three lanes — Twin, Guardian, Brain — over any directory of run logs, with the nine
explainability answers behind every decision and the operator's controls:

```bash
make timeline logs=logs      # http://localhost:8120
```

### Triplet: the Brain on the network

Kafka in, the Twin's MCP server out, the query API and the read-only MCP face up:

```bash
BRAIN_OPERATOR_TOKEN=change-me python -m city_brain --config config/triplet.yaml
```

Or with the partners in containers:

```bash
docker compose --profile triplet up        # joins the external twin_network
docker compose --profile mocks-kafka up    # a broker and our own mock Twin and Guardian
docker compose --profile standalone up     # one scenario, then exit
```

## Tests

```bash
make test                          # everything; the scenario runs make it slow
python -m pytest tests/core -q     # the decision core on its own, in seconds
python -m pytest conformance -q    # the kit a partner can run against itself
```

Tests that need a partner skip themselves with a reason when the endpoint is not reachable:
`BRAIN_TWIN_MCP` for the Twin, `BRAIN_KAFKA_BOOTSTRAP` for Kafka.

## Layout

| Path | What |
|---|---|
| `src/city_brain/core/` | the decision core: pure, deterministic, no clock and no I/O |
| `src/city_brain/edge/` | the guards and partner profiles: nothing enters unvalidated |
| `src/city_brain/adapters/` | files, Kafka, MCP, REST — the outside world |
| `src/city_brain/runtime/` | the loop that joins them, standalone or on the triplet |
| `src/city_brain/persistence/` | the journal and outbox a restart is rebuilt from |
| `policy/`, `params/`, `profiles/` | every threshold, weight and partner-specific word |
| `scenarios/`, `mocks/` | the Test Book and the partners it runs against |
| `timeline/` | the operator's page |
| `conformance/`, `contracts/`, `schemas/` | what we promise and what we require |

## How it decides

1. **Edge** — every message is schema-checked and signature-checked, and rejected with a
   reason code if it is not what the contract says.
2. **Estimate** — each channel is filtered: a tank by a two-state Kalman filter, everything
   else by a lag filter with a rolling baseline. An unjudged or distrusted reading moves the
   prediction but never the estimate.
3. **Detect and assess** — per-domain findings become belief in log-odds, with contradicting
   sensors subtracting and the Guardian's own findings never counted twice.
4. **Plan** — the water channels by model predictive control (or bang-bang as the Tier T0
   fallback); everything else from the declarative response catalog in `policy/default.yaml`.
5. **Shield** — ten static checks, including what the current mode allows.
6. **Trust gate** — R1 needs a trusted reading, R2 needs all of them trusted, R3 needs the
   operator.
7. **Verify** — every command states the effect it expects and the window to see it in; a
   command with no measured effect condemns the actuator and is never resent.
