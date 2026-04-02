# CONTRIBUTING

Start your Grafana plugin develop by following the [6 tips for improving your Grafana plugin before you publish](https://grafana.com/blog/2021/01/21/6-tips-for-improving-your-grafana-plugin-before-you-publish/).

## Development Setup

This plugin requires node >=12.0.0, Go >= 1.7 .

Install Go by following the [installation document](https://golang.org/doc/install):

```sh
wget -c https://golang.org/dl/go1.17.2.linux-amd64.tar.gz
sudo tar -xvf go1.17.2.linux-amd64.tar.gz -C /usr/local/
export PATH=$PATH:/usr/local/go/bin
# or to your local directory
#tar -xvf go1.17.2.linux-amd64.tar.gz -C $HOME/.local/
#export PATH=$PATH:$HOME/.local/go/bin
```

In order to use the build target provided by the plug-in SDK, you can use [mage](https://github.com/magefile/mage) to build the executor binaries.

Install mage with the prebuilt binary:

```sh
wget -c https://github.com/magefile/mage/releases/download/v1.11.0/mage_1.11.0_Linux-64bit.tar.gz
tar -xvf mage_1.11.0_Linux-64bit.tar.gz -C node_modules/.bin/ mage
```

Or use git version, build with next steps, and copy `mage` to your `$PATH`.

```sh
git clone https://github.com/magefile/mage ../mage
cd ../mage
go run bootstrap.go
```

Run the following to update Grafana plugin SDK for Go dependency to the latest minor version:

```sh
go get -u github.com/grafana/grafana-plugin-sdk-go
go mod tidy
```

And then build the plugin:

```sh
npm install -g yarn
yarn install
yarn build:all
```

If you'd like use it somewhere else, just copy or link the `dist/` as `/var/lib/grafana/plugins/tdengine-datasource`.

You can build the plugin package with:

```sh
yarn package
```

It will generate an zip package for the current version of plugin, named like `tdengine-datasource-v3.1.0.zip`.

See [Build backend plugin binaries](https://grafana.com/tutorials/build-a-data-source-backend-plugin/) for more details for datasource backend plugin.
