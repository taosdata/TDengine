/*!
 * The MIT License
 * 
 * Copyright (c) 2008 Jason Frame (jason@onehackoranother.com)
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
pv.Behavior.tipsy = function(opts) {
  var tip;

  /**
   * @private When the mouse leaves the root panel, trigger a mouseleave event
   * on the tooltip span. This is necessary for dimensionless marks (e.g.,
   * lines) when the mouse isn't actually over the span.
   */
  function trigger() {
    $(tip).tipsy("hide");
  }

  /**
   * @private When the mouse leaves the tooltip, remove the tooltip span. This
   * event handler is declared outside of the event handler such that duplicate
   * registrations are ignored.
   */
  function cleanup() {
    if (tip) {
      tip.parentNode.removeChild(tip);
      tip = null;
    }
  }

  return function(d) {
      /* Compute the transform to offset the tooltip position. */
      var t = pv.Transform.identity, p = this.parent;
      do {
        t = t.translate(p.left(), p.top()).times(p.transform());
      } while (p = p.parent);

      /* Create and cache the tooltip span to be used by tipsy. */
      if (!tip) {
        var c = this.root.canvas();
        c.style.position = "relative";
        $(c).mouseleave(trigger);

        tip = c.appendChild(document.createElement("div"));
        tip.style.position = "absolute";
        $(tip).tipsy(opts);
      }

      /* Propagate the tooltip text. */
      tip.title = this.title() || this.text();

      /*
       * Compute bounding box. TODO support area, lines, wedges, stroke. Also
       * note that CSS positioning does not support subpixels, and the current
       * rounding implementation can be off by one pixel.
       */
      if (this.properties.width) {
        tip.style.width = Math.ceil(this.width() * t.k) + 1 + "px";
        tip.style.height = Math.ceil(this.height() * t.k) + 1 + "px";
      } else if (this.properties.radius) {
        var r = this.radius();
        t.x -= r;
        t.y -= r;
        tip.style.height = tip.style.width = Math.ceil(2 * r * t.k) + "px";
      }
      tip.style.left = Math.floor(this.left() * t.k + t.x) + "px";
      tip.style.top = Math.floor(this.top() * t.k + t.y) + "px";

      /*
       * Cleanup the tooltip span on mouseout. Immediately trigger the tooltip;
       * this is necessary for dimensionless marks.
       */
      $(tip).mouseleave(cleanup).tipsy("show");
    };
};
