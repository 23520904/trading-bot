# 01 L1 System Context

> Generated from the .drawio file - do not edit by hand.

## Elements

- Coinbase - [External System: market-data feed] - Public WSS channels (trades, ticker) + REST (candles, trades)
- Trader / Researcher
- Crypto Intelligence Platform - [Software System] - Ingests market data, computes candles & features, runs quant research, ML and PAPER trading; serves dashboards and APIs.
- Slack / PagerDuty - [External System: alert receiver] - Receives alerts from Alertmanager
- Platform Operator (SRE)
- GitHub - [External System: repo + Actions + GHCR] - Source, CI, container registry; Argo CD pulls manifests

## Connections (from -> to : label)

- Trader / Researcher -> Crypto Intelligence Platform : Views dashboards, runs backtests, - monitors paper P&L [HTTPS / WSS] (sync)
- Platform Operator (SRE) -> Crypto Intelligence Platform : Operates: DLQ replay, Grafana, - rollouts [HTTPS] (sync)
- Crypto Intelligence Platform -> Coinbase : Subscribes to live trades [WSS] - Pulls history [HTTPS REST] (sync)
- Crypto Intelligence Platform -> Slack / PagerDuty : Sends alerts [Webhook] (sync)
- Crypto Intelligence Platform -> GitHub : Pulls manifests / images [HTTPS] (sync)

## Tables, notes and text blocks

L1 - System Context
Crypto Intelligence Platform (CIP) - users and external systems - v2.0 - 21 Nov 2026 - arrow = caller -> callee

Out of scope (this release)
Live order routing, custody, fiat on/off-ramp. Coinbase public channels need no trading credentials (ADR-10).
