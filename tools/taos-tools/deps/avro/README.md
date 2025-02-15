
> [!IMPORTANT]  
> The Rust SDK is moving to https://github.com/apache/avro-rs. Please use it for [new issues](https://github.com/apache/avro-rs/issues/new)
 and [pull requests](https://github.com/apache/avro-rs/pulls)!
 
 Apache Avro™<img align="right" height="160" src="doc/assets/images/logo.svg" alt="Avro Logo"/>
============

### Current CI status (Github servers)
[![test c][test c img]][test c]
[![test c#][test c# img]][test c#]
[![test c++][test c++ img]][test c++]
[![test java][test java img]][test java]
[![test javascript][test javascript img]][test javascript]
[![test perl][test perl img]][test perl]
[![test ruby][test ruby img]][test ruby]
[![test python][test python img]][test python]
[![test php][test php img]][test php]

### Current CI status (ARM based servers)
[![test c ARM][test c ARM img]][test c ARM]
[![test c# ARM][test c# ARM img]][test c# ARM]
[![test c++ ARM][test c++ ARM img]][test c++ ARM]
[![test java ARM][test java ARM img]][test java ARM]
[![test javascript ARM][test javascript ARM img]][test javascript ARM]
[![test perl ARM][test perl ARM img]][test perl ARM]
[![test ruby ARM][test ruby ARM img]][test ruby ARM]
[![test python ARM][test python ARM img]][test python ARM]
[![test php ARM][test php ARM img]][test php ARM]

### Current CodeQL status
[![codeql c#][codeql c# img]][codeql c#]
[![codeql java][codeql java img]][codeql java]
[![codeql javascript][codeql javascript img]][codeql javascript]
[![codeql python][codeql python img]][codeql python]

-----

Apache Avro™ is a data serialization system.

Learn more about Avro, please visit our website at:

  https://avro.apache.org/

To contribute to Avro, please read:

  https://cwiki.apache.org/confluence/display/AVRO/How+To+Contribute


<!-- Arranged this way for easy copy-pasting and editor string manipulation -->

[test c]:          https://github.com/apache/avro/actions/workflows/test-lang-c.yml
[test c#]:         https://github.com/apache/avro/actions/workflows/test-lang-csharp.yml
[test c++]:        https://github.com/apache/avro/actions/workflows/test-lang-c++.yml
[test java]:       https://github.com/apache/avro/actions/workflows/test-lang-java.yml
[test javascript]: https://github.com/apache/avro/actions/workflows/test-lang-js.yml
[test perl]:       https://github.com/apache/avro/actions/workflows/test-lang-perl.yml
[test ruby]:       https://github.com/apache/avro/actions/workflows/test-lang-ruby.yml
[test python]:     https://github.com/apache/avro/actions/workflows/test-lang-py.yml
[test php]:        https://github.com/apache/avro/actions/workflows/test-lang-php.yml

[test c ARM]:          https://github.com/apache/avro/actions/workflows/test-lang-c-ARM.yml
[test c# ARM]:         https://github.com/apache/avro/actions/workflows/test-lang-csharp-ARM.yml
[test c++ ARM]:        https://github.com/apache/avro/actions/workflows/test-lang-c++-ARM.yml
[test java ARM]:       https://github.com/apache/avro/actions/workflows/test-lang-java-ARM.yml
[test javascript ARM]: https://github.com/apache/avro/actions/workflows/test-lang-js-ARM.yml
[test perl ARM]:       https://github.com/apache/avro/actions/workflows/test-lang-perl-ARM.yml
[test ruby ARM]:       https://github.com/apache/avro/actions/workflows/test-lang-ruby-ARM.yml
[test python ARM]:     https://github.com/apache/avro/actions/workflows/test-lang-py-ARM.yml
[test php ARM]:        https://github.com/apache/avro/actions/workflows/test-lang-php-ARM.yml

[codeql c#]:         https://github.com/apache/avro/actions/workflows/codeql-csharp-analysis.yml
[codeql java]:       https://github.com/apache/avro/actions/workflows/codeql-java-analysis.yml
[codeql javascript]: https://github.com/apache/avro/actions/workflows/codeql-js-analysis.yml
[codeql python]:     https://github.com/apache/avro/actions/workflows/codeql-py-analysis.yml

[test c img]:          https://github.com/apache/avro/actions/workflows/test-lang-c.yml/badge.svg
[test c# img]:         https://github.com/apache/avro/actions/workflows/test-lang-csharp.yml/badge.svg
[test c++ img]:        https://github.com/apache/avro/actions/workflows/test-lang-c++.yml/badge.svg
[test java img]:       https://github.com/apache/avro/actions/workflows/test-lang-java.yml/badge.svg
[test javascript img]: https://github.com/apache/avro/actions/workflows/test-lang-js.yml/badge.svg
[test perl img]:       https://github.com/apache/avro/actions/workflows/test-lang-perl.yml/badge.svg
[test ruby img]:       https://github.com/apache/avro/actions/workflows/test-lang-ruby.yml/badge.svg
[test python img]:     https://github.com/apache/avro/actions/workflows/test-lang-py.yml/badge.svg
[test php img]:        https://github.com/apache/avro/actions/workflows/test-lang-php.yml/badge.svg

[test c ARM img]:          https://github.com/apache/avro/actions/workflows/test-lang-c-ARM.yml/badge.svg
[test c# ARM img]:         https://github.com/apache/avro/actions/workflows/test-lang-csharp-ARM.yml/badge.svg
[test c++ ARM img]:        https://github.com/apache/avro/actions/workflows/test-lang-c++-ARM.yml/badge.svg
[test java ARM img]:       https://github.com/apache/avro/actions/workflows/test-lang-java-ARM.yml/badge.svg
[test javascript ARM img]: https://github.com/apache/avro/actions/workflows/test-lang-js-ARM.yml/badge.svg
[test perl ARM img]:       https://github.com/apache/avro/actions/workflows/test-lang-perl-ARM.yml/badge.svg
[test ruby ARM img]:       https://github.com/apache/avro/actions/workflows/test-lang-ruby-ARM.yml/badge.svg
[test python ARM img]:     https://github.com/apache/avro/actions/workflows/test-lang-py-ARM.yml/badge.svg
[test php ARM img]:        https://github.com/apache/avro/actions/workflows/test-lang-php-ARM.yml/badge.svg

[codeql c# img]:         https://github.com/apache/avro/actions/workflows/codeql-csharp-analysis.yml/badge.svg
[codeql java img]:       https://github.com/apache/avro/actions/workflows/codeql-java-analysis.yml/badge.svg
[codeql javascript img]: https://github.com/apache/avro/actions/workflows/codeql-js-analysis.yml/badge.svg
[codeql python img]:     https://github.com/apache/avro/actions/workflows/codeql-py-analysis.yml/badge.svg

You can use devcontainers to develop Avro:

* [![Open in Visual Studio Code](https://img.shields.io/static/v1?label=&message=Open%20in%20Visual%20Studio%20Code&color=blue&logo=visualstudiocode&style=flat)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/apache/avro)
* [![Open in Github Codespaces](https://img.shields.io/static/v1?label=&message=Open%20in%20Github%20Codespaces&color=2f362d&logo=github)](https://codespaces.new/apache/avro?quickstart=1&hide_repo_select=true)


### Trademark & logos
Apache®, Apache Avro and the Apache Avro airplane logo are trademarks of The Apache Software Foundation.

The Apache Avro airplane logo on this page has been designed by [Emma Kellam](https://github.com/emmak3l) for use by this project.
