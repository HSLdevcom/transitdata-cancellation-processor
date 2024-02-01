# transitdata-cancellation-processor
To manage cancellations from two sources

- single cancellations from transitdata-omm-cancellation-source
- mass cancellations from transitdata-omm-alert source

Mass cancellations have to be filtered from other alerts.

Trips are queried using route identifier and time span.

Produces single trip-update messages.
