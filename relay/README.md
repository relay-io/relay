# Relay

This contains a simple, no-nonsense, stateless job runner.

### Features
Optional features:
- [`metrics-prometheus`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
- [`backend-postgres`][]: Enables the Postgres backend (default).

[`backend-postgres`]: `relay_postgres`
[`metrics-prometheus`]: https://crates.io/crates/metrics-exporter-prometheus

#### Requirements
- Postgres 9.5+

#### HTTP API
For details about the API see [here](../relay-http/V2-API.md). 

#### How to build
```shell
~ cargo build -p relay --release
```

#### Clients
Here is a list of existing clients.

| Language                                                                                 | Description                 |
|------------------------------------------------------------------------------------------|-----------------------------|
| [Go](https://github.com/relay-io/relay-sdk-go)                                           | Go low & high level client. |
| [Rust](https://github.com/relay-io/relay/blob/main/relay-http/src/http/client/client.rs) | Rust client and consumer.   |


#### License

<sup>
Licensed under <a href="LICENSE">GNU AFFERO GENERAL PUBLIC LICENSE 3.0 or later</a>

<br>

<sub>
Any contribution intentionally submitted for inclusion by you shall be licensed as above, without any additional terms or conditions.
</sub>
