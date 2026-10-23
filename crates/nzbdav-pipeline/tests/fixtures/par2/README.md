# PAR2 fixtures

Copied from the `rust-par2` 0.1.2 crate's test fixtures (MIT OR Apache-2.0),
which were generated with par2cmdline-turbo.

- `testdata.bin`, `testdata.bin.par2`, `testdata.bin.vol0+1.par2`: a 100000-byte
  file, its index and one recovery volume (`tests/fixtures/intact`).
- `other.par2`: the index of a different set protecting a 200000-byte file of
  the same name (`tests/fixtures/unrepairable/testdata.bin.par2`).
