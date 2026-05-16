package trivy

import data.lib.trivy

default ignore = false

# Ignore a license
ignore {
    input.PkgName == "tdengine-datasource"
    input.Name == "AGPL-3.0-only"
}

ignore {
    input.PkgName == "github.com/hashicorp/yamux"
    input.Name == "MPL-2.0"
}

ignore {
    input.PkgName == "github.com/hashicorp/go-plugin"
    input.Name == "MPL-2.0"
}

ignore {
    input.Name == "AGPL-3.0"
    input.FilePath == "LICENSE"
}

ignore {
    input.Name == "AGPL-3.0"
    input.FilePath == "dist/LICENSE"
}

ignore {
    input.Name == "AGPL-3.0"
    input.FilePath == "examples/collectd/LICENSE"
}

ignore {
    input.Name == "AGPL-3.0"
    input.FilePath == "examples/telegraf/LICENSE"
}

