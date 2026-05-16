#!/bin/sh
name=`jq '.id' -r src/plugin.json`
zipname=`jq '.id + "-" + .info.version + ".zip"' -r src/plugin.json`
echo generate plugin as $name
rm -rf $name
cp -r dist $name
zip -r $zipname $name
echo packaged to $zipname
