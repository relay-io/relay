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


### Migration v1 -> v2
The v2 release of Relay is a complete rewrite of the v1 release, while maintaining backward compatibility with the v1 API.

The original v1 Relay [here](https://github.com/rust-playground/relay-rs) was running flawlessly in production for over 
a year. The main reason for a v2 was to add some additional features making it easier interacting with in-flight jobs, 
but improvements also include:
- Allowing updating of in-flight jobs leveraging the new `run_id` so that already in-flight job interactions will cease upon the next interaction with the Relay server.
- Added `requeue` endpoint, replacing the old reschedule, to allow not only rescheduling but also atomically creating new even unrelated jobs.
- Added enqueue + requeue mode to better chose guarantees when creating/recreating/replacing jobs.


So the API is backward compatible, but some database changes are not, please read this carefully before upgrading. 
- It is recommended to back up your database prior to migrating just in case.
- `queue` and `id` columns are now restricted to `1024 characters` to limit the index sizes, prevent hitting max index size in PG and leave some overhead for future improvements.
- `max_retries` becomes nullable which now represents infinite retries. Any previous < 0 will be set to null during the migration.
- `max_retries` has also been changed to be a `smallint` and now has a maximum value of `32767` which is more than enough for most use cases.
- `timeout` and `max_retries` will now be validated to be >= 0, which they always should be.


#### License

Unless otherwise explicitly stated the below license applies to all code.

<sup>
Licensed under <a href="LICENSE">GNU AFFERO GENERAL PUBLIC LICENSE 3.0 or later</a>

<br>

<sub>
Any contribution intentionally submitted for inclusion by you shall be licensed as above, without any additional terms or conditions.
</sub>
