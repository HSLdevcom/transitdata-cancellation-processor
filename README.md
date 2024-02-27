# transitdata-cancellation-processor [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-cancellation-processor/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-cancellation-processor/actions/workflows/test-and-build.yml)

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

This application manages cancellations from two sources

- single cancellations from transitdata-omm-cancellation-source
- mass cancellations from transitdata-omm-alert source

Mass cancellations have to be filtered from other alerts.

Trips are queried from Digitransit API using GraphQL. Route identifier and time span are used as search criteria.

Produces single trip-update messages.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

### Environment variables

* `PROCESSOR_TIMEZONE`: timezone to use in the cancellation processor (default: "Europe/Helsinki")

### Secrets

* `Digitransit API URI`