# Relay
A simple, no-nonsense, stateless job runner.

A Job runner is like a Relay race, you hand the baton off in turn to be handed off again and so on until the end of the race. 
The race itself doesn't record the times, video itself or announce the outcomes; but rather those are done externally.

To that end Relay is designed to be simple, reliable and easy to use. 
It on purpose does not:
- Track `Job` history.
- Keep logs.
- Have any notion of success or fail.
- Retain Job run information after it completes.

Relay embraces the unix philosophy of doing one thing and doing it well leaving the above as optional features to be handled
by the callers and or clients.

#### Crates
See details for each crate.

| Crate                                        | Description                                  | License           |
|----------------------------------------------|----------------------------------------------|-------------------|
| [relay](./relay/README.md)                   | A simple, no-nonsense, stateless job runner. | AGPL-3.0-or-later |
| [relay-core](./relay-core/README.md)         | Contains all core shared code and logic.     | MIT OR Apache-2.0 |
| [relay-http](./relay-http/README.md)         | HTTP frontend for Relay.                     | MIT OR Apache-2.0 |
| [relay-client](./relay-client/README.md)     | Relay client for Rely frontends.             | AGPL-3.0-or-later |          
| [relay-postgres](./relay-postgres/README.md) | Postgres backend for Relay.                  | AGPL-3.0-or-later |

#### License

Unless otherwise explicitly stated the below license applies to all code.

<sup>
Licensed under <a href="LICENSE">GNU AFFERO GENERAL PUBLIC LICENSE 3.0 or later</a>

<br>

<sub>
Any contribution intentionally submitted for inclusion by you shall be licensed as above, without any additional terms or conditions.
</sub>
