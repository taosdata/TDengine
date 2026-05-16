module.exports = {
  names: ["no-angle-bracket-url"],
  description: "Disallow angle bracket auto-links",
  tags: ["links"],
  function: function params(params, onError) {
    params.tokens.forEach(token => {
      if (token.type === "inline" && token.content.match(/<https?:\/\/[^>]+>/)) {
        onError({
          lineNumber: token.lineNumber,
          detail: "Do not use angle bracket auto-links. Use [text](url) instead."
        });
      }
    });
  }
};