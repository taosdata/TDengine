# Apache Yetus integration

The plugin under `buildtest.sh` will provide the plugin to run the base `build.sh test` which will trigger the tests of the main Avro projects, and the sections below `lang/*/build.sh`. To run this, use:

```bash
test-patch --plugins=buildtest --user-plugins=share/precommit/ --run-tests --empty-patch --docker --dockerfile=share/docker/Dockerfile --dirty-workspace --verbose=true
```

Note, that this is still a very crude implementation of Apache Yetus, and in the future we would like to refine this to provide plugins for the different languages.
