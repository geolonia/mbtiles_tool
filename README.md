# mbtiles_tool

Miscellaneous tools for working with mbtiles archives

Subcommands:

* `subdivide` - split a single mbtiles archive into several subarchives (see `files/subdivide_*` for example configurations)
* `statistics` - show information about a mbtiles archive. Currently calculates min/max/average tile_data sizes and shows specific tiles larger than 400KB and 500KB.
* `overzoom` - generate overzoomed tiles from a source mbtiles archive. Useful when combined with tile-join on tilesets with differing base zooms

Run `mbtiles_tool help` for more information.
