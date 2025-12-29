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
            // Check for `strong_close` and validate the preceding content
            if (child.type === "strong_close") {
              const precedingToken = token.children[childIndex - 1];
              const nextToken = token.children[childIndex + 1];

              if (precedingToken && precedingToken.type === "text") {
                const boldContent = precedingToken.content.trim();
                const punctuationRegex = /[：:；;,\.]$/; // Match full-width and half-width punctuation at the end

                if (punctuationRegex.test(boldContent)) {
                  if (!nextToken || nextToken.type !== "text" || !nextToken.content.startsWith(" ")) {
                    const lineContent = params.lines[child.lineNumber - 1];
                    const boldStartIndex = lineContent.indexOf(`**${boldContent}**`);
                    const columnNumber = boldStartIndex + `**${boldContent}**`.length + 1;
                    const afterBold = lineContent.slice(boldStartIndex + `**${boldContent}**`.length);

                    if (afterBold && afterBold.trim().length > 0 && !afterBold.startsWith(" ")) {
                      const columnNumber = boldStartIndex + `**${boldContent}**`.length + 1;
                      onError({
                        lineNumber: child.lineNumber || token.lineNumber,
                        detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
                        context: lineContent.trim(),
                        fixInfo: {
                          editColumn: columnNumber, // Correctly specify the column for the edit
                          insertText: " ", // Suggest inserting a space
                        },
                      });
                    }
                  }
                }
              }
            }

            // Additional check for `text` tokens containing `**` markers
            if (child.type === "text" && child.content.includes("**")) {
              const regex = /\*\*(.*?)\*\*/g; // Match **bold** and extract content
              let match;
              while ((match = regex.exec(child.content)) !== null) {
                const boldContent = match[1]; // Extract content inside ** **
                const punctuationRegex = /[：:；;,\.]$/; // Match full-width and half-width punctuation at the end

                if (punctuationRegex.test(boldContent)) {
                  const remainingContent = child.content.slice(match.index + match[0].length);
                  if (!remainingContent.startsWith(" ")) {
                    const lineContent = params.lines[child.lineNumber - 1];
                    const boldStartIndex = lineContent.indexOf(match[0]);
                    const columnNumber = boldStartIndex + match[0].length + 1;
                    const afterBold = lineContent.slice(boldStartIndex + `**${boldContent}**`.length);

                    if (afterBold && afterBold.trim().length > 0 && !afterBold.startsWith(" ")) {
                      const columnNumber = boldStartIndex + `**${boldContent}**`.length + 1;
                      onError({
                        lineNumber: child.lineNumber || token.lineNumber,
                        detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
                        context: lineContent.trim(),
                        fixInfo: {
                          editColumn: columnNumber, // Correctly specify the column for the edit
                          insertText: " ", // Suggest inserting a space
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