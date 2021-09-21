PR['registerLangHandler'](
  PR['createSimpleLexer'](
    [
      // A comment is either a line comment that starts with two dashes, or
      // two dashes preceding a long bracketed block.
      [PR['TAOSDATA_TERMINAL'], /^(*?)/]
    ]
  ),
['terminal','term']);