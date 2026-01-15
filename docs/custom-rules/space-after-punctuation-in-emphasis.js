"use strict";

module.exports = [
  {
    names: ["space-after-punctuation-in-emphasis"],
    description: "Ensure there is a space after emphasis markers (**bold:** text) if the emphasized text ends with punctuation (e.g., :, ：, ;, ；, etc.) and is immediately followed by non-whitespace content.",
    tags: ["emphasis", "formatting"],
    function: function spaceAfterPunctuationInEmphasis(params, onError) {
      function escapeRegExp(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      }

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
                    const lineContent = params.lines[child.lineNumber - 1] || "";
                    const boldPattern = new RegExp("\\*\\*" + escapeRegExp(boldContent) + "\\*\\*");
                    const m = lineContent.match(boldPattern);
                    const afterBold = m ? lineContent.slice(lineContent.indexOf(m[0]) + m[0].length) : "";

                    // skip if afterBold is end of line/only whitespace, or followed by another emphasis marker (**)
                    if (!afterBold || afterBold.trim().length === 0 || afterBold.startsWith("**")) {
                      // skip
                    } else if (!afterBold.startsWith(" ")) {
                      const columnNumber = (m ? lineContent.indexOf(m[0]) : 0) + (m ? m[0].length : (`**${boldContent}**`.length)) + 1;
                      onError({
                        lineNumber: child.lineNumber || token.lineNumber,
                        detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
                        context: lineContent.trim(),
                        fixInfo: {
                          editColumn: columnNumber,
                          deleteCount: 0,
                          insertText: " ",
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
              const lineContent = params.lines[child.lineNumber - 1] || "";
              while ((match = regex.exec(child.content)) !== null) {
                const boldContent = match[1]; // Extract content inside ** **
                const punctuationRegex = /[：:；;,\.]$/; // Match full-width and half-width punctuation at the end

                if (punctuationRegex.test(boldContent)) {
                  const remainingContent = child.content.slice(match.index + match[0].length);
                  // remainingContent may be empty if bold is at end of this token
                  // locate the actual bold in the full line to get correct afterBold
                  const boldPattern = new RegExp("\\*\\*" + escapeRegExp(boldContent) + "\\*\\*");
                  const m = lineContent.match(boldPattern);
                  const afterBold = m ? lineContent.slice(lineContent.indexOf(m[0]) + m[0].length) : "";

                  // skip if afterBold is end of line/only whitespace, or followed by another emphasis marker (**)
                  if (!afterBold || afterBold.trim().length === 0 || afterBold.startsWith("**")) {
                    // skip
                  } else if (!afterBold.startsWith(" ")) {
                    const columnNumber = (m ? lineContent.indexOf(m[0]) : 0) + (m ? m[0].length : (`**${boldContent}**`.length)) + 1;
                    onError({
                      lineNumber: child.lineNumber || token.lineNumber,
                      detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
                      context: lineContent.trim(),
                      fixInfo: {
                        editColumn: columnNumber,
                        deleteCount: 0,
                        insertText: " ",
                      },
                    });
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