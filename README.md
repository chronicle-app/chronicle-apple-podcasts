# Chronicle::ApplePodcasts
[![Gem Version](https://badge.fury.io/rb/chronicle-apple-podcasts.svg)](https://badge.fury.io/rb/chronicle-apple-podcasts)

Extract your Apple Podcasts listening history with this plugin for [chronicle-etl](https://github.com/chronicle-app/chronicle-etl)

## Available Connectors
### Extractors
- `listens` - Extractor for podcast episode listening history

### Transformers
- `listen` - Transforms a listen into Chronicle Schema

## Usage

```sh
# Install chronicle-etl and then this plugin
$ gem install chronicle-etl
$ chronicle-etl connectors:install apple-podcasts

# Extract all history
$ chronicle-etl --extractor apple-podcasts:listens

# Get last week of history and transform it into Chronicle Schema
$ chronicle-etl --extractor apple-podcasts:listens --since 1w --transformer apple-podcasts
```
