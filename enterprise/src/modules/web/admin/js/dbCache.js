/**
 * we just cache the dbs, tables, and other querys into the cache
 * while query, new cache 
 * while next or end, fetch from the cache, if to the end, new query then append to cache
 * while prev or first, fetch from the cache
 **/
 
var DbCache = new function() {
 
	//total rows 
	this.cacheTotal = 0;

	//the next rows position
	this.pagesIndex = 0;
	this.pagesTotal = 0;

	//the data rows
	this.cacheContent = [];

	//the data head
	this.cacheHead = [];

	//default rows per page
	this.rowPerPage = 15;

	/**
	 * Assert the cache content like this
	 * head -> []
	 * rows -> integer
	 * data -> [[], []]
	 **/
	this.Append = function(d)
	{
		if (d == null) {
			return;
		}
		if (d.rows != d.data.length) {
			DbUtil.Error(DB_CODE_CACHE_INVALID_ROW);
		}
		
		this.cacheTotal += d.rows;
		this.cacheHead = d.head;
		
		var remain = this.cacheTotal % this.rowPerPage;
		this.pagesTotal = parseInt(this.cacheTotal / this.rowPerPage);
		if (remain > 0) {
			this.pagesTotal++;
		}
		
		if (d.rows != 0) {
			this.cacheContent = this.cacheContent.concat(d.data);
		}
		
		if (this.cacheTotal != this.cacheContent.length) {
			DbUtil.Error(DB_CODE_CACHE_INVALID_ROW);
		}
	}

	this.AppendForNext = function(d)
	{
		if (d == null) {
			return;
		}
		if (d.rows != d.data.length) {
			DbUtil.Error(DB_CODE_CACHE_INVALID_ROW);
		}
		
		var timestamp = "";
		if (this.cacheTotal != 0) {
			timestamp = this.cacheContent[this.cacheTotal - 1][0];
		}
		
		var pos = -1;
		for (var i = 0; i < d.data.length; ++i) {
			var tt = d.data[i][0];
			if (timestamp == tt) {
				pos = i;
				break;
			}
		}
		
		if (pos == -1) {
			return;
		}
		
		pos++;
		
		for (i = pos; i < d.data.length; ++i) {
			var dd = d.data[i];
			this.cacheContent.push(dd);
		}
		
		this.cacheTotal = this.cacheContent.length;
		this.cacheHead = d.head;
		
		var remain = this.cacheTotal % this.rowPerPage;
		this.pagesTotal = parseInt(this.cacheTotal / this.rowPerPage);
		if (remain > 0) {
			this.pagesTotal++;
		}
		
	}

	this.Clear = function() 
	{
		this.cacheTotal = 0;
		this.pagesIndex = 0;
		this.pagesTotal = 0;
		this.cacheContent = [];
	}

	this.Fetch = function()
	{
		var d = {};
		d.head = this.cacheHead;
		d.data = [];
		
		if (this.pagesIndex < 0 || this.pagesIndex >= this.pagesTotal) {
			return d;
		}
		
		var beginPos = Math.max(0, this.pagesIndex) * this.rowPerPage;
		var endPos = Math.min( (this.pagesIndex + 1) * this.rowPerPage, this.cacheTotal);
		for (var i = beginPos; i < endPos; ++i)
		{
			d.data.push(this.cacheContent[i]);
		}
		
		return d;
	}

	this.HasNext = function()
	{
		if (this.pagesIndex < this.pagesTotal - 1) {
			this.pagesIndex ++;
			return true;
		}
		//DbUtil.Error(DB_CODE_NO_DATAS);
		return false;
	}

	this.HasPrev = function()
	{
		if (this.pagesIndex <= 0) {
			return false;
		}
		this.pagesIndex--;
		return true;
	}

	this.HasFirst = function()
	{
		if (this.pagesTotal <= 0) {
			return false;
		}
		this.pagesIndex = 0;
		return true;
	}

	this.HasEnd = function()
	{
		if (this.cacheTotal <= 0) {
			return false;
		}
		this.pagesIndex = this.pagesTotal -1;
		return true;
	}

	this.GetRowIndex = function(rowIndex)
	{
		return this.pagesIndex * this.rowPerPage + rowIndex + 1;
	}

	this.RowsPerPage = function()
	{
		return this.rowPerPage;
	}

	this.PrepareCsvData = function()
	{
		//var str = "col1,col2,col3\nvalue1,value2,value3";  
		//str =  encodeURIComponent(str); 
		// this.cacheHead = ['11','22'];
		// this.cacheContent = [['a', 'b'],['c', 'd'],['e', 'f']];
		
		var head = this.cacheHead.join(",");
		var contents = [];
		var length= this.cacheContent.length;
		for (var i = 0; i < this.cacheContent.length; ++i) {
			contents.push(this.cacheContent[i].join(","));
		}
		var ret = head + "\n" + contents.join("\n");
		// var ret = head + "\n'" + contents.join("'\n'") + "'";
		return ret;
	}

	this.CacheToCsv = function() 
	{
		var str = this.PrepareCsvData();
		var blob = new Blob([str], { type: 'text/csv' }); //new way  
		var csvUrl = URL.createObjectURL(blob);  
		document.getElementById("download").href = csvUrl;  
		document.getElementById("download").click();
	}

	this.Next = function() 
	{
		if (this.HasNext()) {
			var d = this.Fetch();
			if (this.fileBodyFp != null) this.fileBodyFp(d);
		}
	}

	this.Prev = function() 
	{
		if (this.HasPrev()) {
			var d = this.Fetch();
			if (this.fileBodyFp != null) this.fileBodyFp(d);
		}
	}

	this.First = function() 
	{
		if (this.HasFirst()) {
			var d = this.Fetch();
			if (this.fileBodyFp != null) this.fileBodyFp(d);
		}
	}

	this.End = function() 
	{
		if (this.HasEnd()) {
			var d = this.Fetch();
			if (this.fileBodyFp != null) this.fileBodyFp(d);
		}
	}

	this.Init = function(fillHeadFp, fileBodyFp)
	{
		this.fillHeadFp = fillHeadFp;
		this.fileBodyFp = fileBodyFp;
	}

	//ajax callback
	this.OnData = function(data)
	{
		//if (data.rows == 0) {
			//dbSuccess(DB_CODE_NO_DATAS);
		//}
		//else {
			this.Append(data);
			var d = this.Fetch();
			if (this.fillHeadFp != null) this.fillHeadFp(d);
			if (this.fileBodyFp != null) this.fileBodyFp(d);
		//}
	}
	
	this.Refresh = function()
	{
		var d = this.Fetch();
		if (this.fillHeadFp != null) this.fillHeadFp(d);
		if (this.fileBodyFp != null) this.fileBodyFp(d);
	}
	
	this.Reload = function(jobj) 
	{
		this.cacheTotal = jobj.cacheTotal;
		this.pagesIndex = jobj.pagesIndex;
		this.pagesTotal = jobj.pagesTotal;
		this.cacheContent = jobj.cacheContent;
		this.cacheHead = jobj.cacheHead;
		this.rowPerPage = jobj.rowPerPage;
	}
};











