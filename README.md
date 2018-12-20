# sat-stac-landsat

This is a repository used for the creation and maintenance of a [STAC](https://github.com/radiantearth/stac-spec) compliant [Landsat catalog](https://landsat-stac.s3.amazonaws.com/catalog.json) for data from the [Landsat on AWS project](https://registry.opendata.aws/landsat-8/) (located at s3://landsat-pds/).

There are two pieces of this repository:

- A Python library (satstac.landsat) containing functions for reading Landsat metadata, transforming to STAC Items, and adding to the Landsat catalog
- An AWS Lambda handler that accepts an SNS message containing the s3 URL for a new Landsat scene and adds it to the Landsat catalog.

## About
[sat-stac-landsat](https://github.com/sat-utils/sat-stac-landsat) was created by [Development Seed](<http://developmentseed.org>) and is part of a collection of tools called [sat-utils](https://github.com/sat-utils).
