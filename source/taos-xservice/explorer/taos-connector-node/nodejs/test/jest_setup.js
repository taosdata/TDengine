// global.TextEncoder = require("util").TextEncoder; 
const NodeEnvironment = require('jest-environment-node');

// A custom environment to set the TextEncoder that is required by mongodb.
module.exports = class CustomTestEnvironment extends NodeEnvironment {
  async setup() {
    await super.setup();
    if (typeof this.global.TextEncoder === 'undefined') {
      const { TextEncoder } = require('util');
      this.global.TextEncoder = TextEncoder;
    }
  }
}