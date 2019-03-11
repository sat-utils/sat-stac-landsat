# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v0.1.1] - 2019-04-11

### Added
- Option to only add items that are missing from catalog
- Added landsat:revision to properties
- Misc example Python scripts for updating elasticsearch

### Changed
- Internal Item ID changed to be Landsat Scene ID without last 5 characters (ground station and revision). This ensures only the single latest revision is in the catalog.


## [v0.1.0] - 2019-01-17

Initial Release

[Unreleased]: https://github.com/sat-utils/sat-stac-landsat/compare/master...develop
[v0.1.1]: https://github.com/sat-utils/sat-stac-landsat/compare/0.1.0...0.1.1
[v0.1.0]: https://github.com/sat-utils/sat-stac-landsat/tree/0.1.0
