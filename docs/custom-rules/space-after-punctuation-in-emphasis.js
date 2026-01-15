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

      const punctuationRegex = /[：:；;,\.]$/;

      function reportIfMissingSpace(lineContent, boldContent, lineNumber) {
        if (!boldContent || !lineContent) return false;
        const pattern = new RegExp("\\*\\*" + escapeRegExp(boldContent) + "\\*\\*", "g");
        let m;
        while ((m = pattern.exec(lineContent)) !== null) {
          const afterBold = lineContent.slice(m.index + m[0].length);
          if (!afterBold || afterBold.trim().length === 0 || afterBold.startsWith("**")) {
            continue;
          }
          if (!afterBold.startsWith(" ")) {
            const columnNumber = m.index + m[0].length + 1;
            onError({
              lineNumber: lineNumber,
              detail: `Add a space after the closing emphasis markers (**), if the emphasis ends with punctuation. (Column: ${columnNumber})`,
              context: lineContent.trim(),
              fixInfo: {
                editColumn: columnNumber,
                deleteCount: 0,
                insertText: " ",
              },
            });
            return true;
          }
          return false;
        }
        return false;
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

                if (punctuationRegex.test(boldContent)) {
                  if (!nextToken || nextToken.type !== "text" || !nextToken.content.startsWith(" ")) {
                    const lineContent = params.lines[child.lineNumber - 1] || "";
                    reportIfMissingSpace(lineContent, boldContent, child.lineNumber);
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
                if (!boldContent) continue;
                if (punctuationRegex.test(boldContent)) {
                  // try to locate an applicable bold occurrence in the full line
                  reportIfMissingSpace(lineContent, boldContent, child.lineNumber);
                }
              }
            }
          });
        }
      });
    },
  },
];