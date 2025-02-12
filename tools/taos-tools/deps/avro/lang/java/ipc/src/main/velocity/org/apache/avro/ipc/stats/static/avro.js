/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function makeDotChart(yVals) {
    var xVals = pv.range(1, yVals.length + 1);
    var data = new Array();
    var dotColors = pv.Colors.category20().range();
    
    for (i = 0; i < yVals.length; i = i + 1) {
      data[i] = {x: xVals[i], y: yVals[i]};
    }

	/* Sizing and scales. */
	var w = 200,
	    h = 250,
	    x = pv.Scale.linear(0, Math.max.apply(Math, xVals)).range(0, w),
	    y = pv.Scale.linear(0, Math.max.apply(Math, yVals)).range(0, h),
	    c = pv.Scale.linear(1, 20).range("orange", "brown");
	
	/* The root panel. */
	var vis = new pv.Panel()
	    .width(w)
	    .height(h)
	    .bottom(20)
	    .left(50)
	    .right(10)
	    .top(5);
	
	/* Y-axis and ticks. */
	vis.add(pv.Rule)
	    .data(y.ticks())
	    .bottom(y)
	    .strokeStyle(function(d) {return d ? "#eee" : "#000"})
	  .anchor("left").add(pv.Label)
	    .text(y.tickFormat);

	
	/* The dot plot! */
	vis.add(pv.Panel)
	    .data(data)
	    .add(pv.Dot)
	    .left(function(d) {return x(d.x)})
	    .bottom(function(d) {return y(d.y)})
	    .strokeStyle(function(d) {return dotColors[d.x % 20]})
	    .fillStyle(function() {return this.strokeStyle().alpha(1)})
	    .title(function(d) {return d.y})
	    .event("mouseover", pv.Behavior.tipsy({gravity: "n", 
	      fade: false, delayIn: 0}));
	vis.render();
}


function makeBarChart(labels, boundries, data) {
	var w = 200,
	    h = 250,
	    x = pv.Scale.ordinal(pv.range(data.length)).splitBanded(0, w, 4/5),
	    y = pv.Scale.linear(0, Math.max.apply(Math, data)).range(0, h),
	    i = -1,
	    c = pv.Scale.linear(1, 5, 20).range("green", "yellow", "red");

	var vis = new pv.Panel()
	    .width(w)
	    .height(h)
	    .bottom(20)
	    .left(40)
	    .right(5)
	    .top(30);
	
	var bar = vis.add(pv.Bar)
	    .data(data)
	    .left(function(){ return x(this.index); })
	    .width(10)
	    .bottom(0)
	    .height(y)
	    .fillStyle(function(d) {return "#1f77b4"})
	    .title(function() { return boundries[this.index]; })
	    .event("mouseover", pv.Behavior.tipsy({gravity: "n", 
	      fade: false, delayIn: 0}));
	
	bar.anchor("bottom").add(pv.Label)
    	.textMargin(5)
		.textBaseline("top")
		.text(function() {return (this.index % 4 == 0) ? labels[this.index]: ""});
	
	vis.add(pv.Rule)
	    .data(y.ticks())
	    .bottom(function(d) {return Math.round(y(d)) - .5})
	    .strokeStyle(function(d) {return d ? "rgba(255,255,255,.3)" : "#000"})
	  .add(pv.Rule)
	    .left(0)
	    .width(5)
	    .strokeStyle("#000")
	  .anchor("left").add(pv.Label)
	    .text(function(d) {return d.toFixed(1)});
	vis.render();
}
