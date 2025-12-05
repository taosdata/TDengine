"use strict";

module.exports = [
  {
    names: ["space-after-punctuation-in-emphasis"],
    description: "Ensure there is a space after emphasis markers (**bold:** text) if the emphasized text ends with punctuation (e.g., :, ：, ;, ；, etc.) and is immediately followed by non-whitespace content.",
    tags: ["emphasis", "formatting"],
    function: function spaceAfterPunctuationInEmphasis(params, onError) {
      params.tokens.forEach((token) => {
        if (token.type === "inline" && token.children) {
          token.children.forEach((child, childIndex) => {
            if (child.type === "strong_close") {
              const precedingToken = token.children[childIndex - 1];
              const nextToken = token.children[childIndex + 1];

              if (precedingToken && precedingToken.type === "text") {
                const boldContent = precedingToken.content.trim();
                const punctuationRegex = /[：:；;,\.]$/;

                if (punctuationRegex.test(boldContent)) {
                  if (!nextToken || nextToken.type !== "text" || !nextToken.content.startsWith(" ")) {
                    const lineContent = params.lines[child.lineNumber - 1];
                    const boldStartIndex = lineContent.indexOf(`**${boldContent}**`);
                    const columnNumber = boldStartIndex + `**${boldContent}**`.length + 1;

                    if (boldStartIndex !== -1) {
                      onError({
                        lineNumber: child.lineNumber || token.lineNumber,
                        detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
                        context: lineContent.trim(),
                        fixInfo: {
                          editColumn: columnNumber,
                          insertText: " ",
                        },
                      });
                    }
                  }
                }
              }
            }

            if (child.type === "text" && child.content.includes("**")) {
              const regex = /\*\*(.*?)\*\*/g;
              let match;
              while ((match = regex.exec(child.content)) !== null) {
                const boldContent = match[1];
                const punctuationRegex = /[：:；;,\.]$/;

                if (punctuationRegex.test(boldContent)) {
                  const remainingContent = child.content.slice(match.index + match[0].length);
                  if (!remainingContent.startsWith(" ")) {
                    const lineContent = params.lines[child.lineNumber - 1];
                    const boldStartIndex = lineContent.indexOf(match[0]);
                    const columnNumber = boldStartIndex + match[0].length + 1;

                    if (boldStartIndex !== -1) {
                      onError({
                        lineNumber: child.lineNumber || token.lineNumber,
                        detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
                        context: lineContent.trim(),
                        fixInfo: {
                          editColumn: columnNumber,
                          insertText: " ",
                        },
                      });
                    }
                  }
                }
              }
            }
          });
        }
      });
    },
  },
];