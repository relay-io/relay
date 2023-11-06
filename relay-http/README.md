# Relay HTTP Frontend

This is the HTTP frontend interface for Relay.


### API
The current version of the API is `v2`. See details [here](./V2-API.md).

The previous version of the API is `v1` and has been made backward compatible into the v2 with one caveat which is the `created_at` timestamp for Jobs will be change when `rescheduled`, which is now `requeue` in the v2 API.
It is highly recommended to upgrade to use the new `v2` API as `v1` is deprecated.

See details for the `V1` API [here](./V1-API.md).