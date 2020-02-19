/**
 * token and user is coming from server
 * it will in localStorage
 */

var DbSession = new function() {
	
	this.SetToken = function(token) {
		localStorage.setItem("dbToken", token);
	}

	this.GetToken = function() {
		token = localStorage.getItem("dbToken");
		if (token == null || token == undefined || token == "undefined")
			return "";
		return token;
	}

	this.SetUser = function(user) {
		localStorage.setItem("dbUser", user);
	}

	this.GetUser = function() {
		user = localStorage.getItem("dbUser");
		if (user == null || user == undefined || user == "undefined")
			return "";
		return user;
	}
	
	// a simple way for authority
	this.IsRoot = function() {
		var user = localStorage.getItem("dbUser");
		return user == "root" || user == "monitor" || user == "stream";
	}

	this.SetDate = function(date) {
		localStorage.setItem("dbDate", date);
	}

	this.GetDate = function() {
		datestr = localStorage.getItem("dbDate");
		if (datestr == null || datestr == undefined || datestr == "undefined")
			return "";
		return datestr;
	}

	this.SetDb = function(db) {
		localStorage.setItem("dbDb", db);
	}

	this.GetDb = function() {
		var db = localStorage.getItem("dbDb");
		if (db == null || db == undefined || db == "undefined" )
			return "";
		return db;
	}

	this.ClearDbList = function()
	{
		localStorage.setItem("dbDbList", "");
	}

	this.GetDbList = function() {
		var str = localStorage.getItem("dbDbList");
		if (str != "" && str != null && str != undefined || str == "undefined") {
			DbCache.Reload(JSON.parse(str));
			return true;
		}
		return false;
	}

	this.SetDbList = function() {
		if (DbCache.cacheTotal != 0) {
			var str = JSON.stringify(DbCache);
			localStorage.setItem("dbDbList", str);
		}
	}

	this.ClearTableList = function()
	{
		localStorage.setItem("dbTableList", "");
	}

	this.GetTableList = function() {
		var str = localStorage.getItem("dbTableList");
		if (str != "" && str != null && str != undefined || str == "undefined") {
			DbCache.Reload(JSON.parse(str));
			return true;
		}
		return false;
	}

	this.SetTableList = function() {
		if (DbCache.cacheTotal != 0) {
			var str = JSON.stringify(DbCache);
			localStorage.setItem("dbTableList", str);
		}
	}

	this.ClearUserList = function()
	{
		localStorage.setItem("dbUserList", "");
	}

	this.GetUserList = function() {
		var str = localStorage.getItem("dbUserList");
		if (str != "" && str != null && str != undefined || str == "undefined") {
			DbCache.Reload(JSON.parse(str));
			return true;
		}
		return false;
	}

	this.SetUserList = function() {
		if (DbCache.cacheTotal != 0) {
			var str = JSON.stringify(DbCache);
			localStorage.setItem("dbUserList", str);
		}
	}

	//cache flag is for dbs.html and tables.html
	//where use DbSession.GetTableList and DbSession.GetDbList
	this.SetCacheFlag = function()
	{
		localStorage.setItem("dbCacheFlag", "true");
	}

	this.GetCacheFlag = function() 
	{
		flag = localStorage.getItem("dbCacheFlag");
		if (flag == "true") {
			this.ClearCacheFlag();
			return true;
		}
		return false;
	}

	this.ClearCacheFlag = function()
	{
		localStorage.setItem("dbCacheFlag", "");
	}

	/**
	 * for logout
	 */
	this.Clear = function()
	{	
		this.SetToken("");
		this.SetUser("");
		this.SetDate("");
		this.SetDb("");
		this.ClearTableList("");
		this.ClearDbList("");
		this.ClearUserList("");
		
		this.Logout();
	}

	this.Logout = function()
	{
		var token = this.GetToken();
		if (token == null || token == "invalid" || token == "" || token == undefined || token == "undefined") {
			return;
		}
		$.ajax({
			type: "post",
			url: "admin/logout",
			headers: {
				'Authorization' : 'Authorization: Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04'  + token
			},
			data: "logout",
			dataType: "json",
			success: function(d) {			
			},
			error: function(msg) { 
				DbUtil.Error(DB_CODE_INVALID_SERVER);
			}		
		});	
	}

	/**
	 * while page load, valid the token and user
	 * if not exist, goto login.html
	 */
	this.ValidSession = function() {
		var user = this.GetUser();
		var token = this.GetToken();
		if (token == null || token == "") {
			DbUtil.GotoPage("login.html");
			//DbUtil.Error(DB_CODE_INVALID_SESSION);
		}
		else if (user == null || user == "") {
			DbUtil.GotoPage("login.html");
			//DbUtil.Error(DB_CODE_INVALID_USER);
		}
		else{
			$('#user').text(user);
			if (this.IsRoot()) {
				var monitorLi = $('#monitorLiId');
				if (monitorLi == null || monitorLi == undefined) return;
				monitorLi.show();
				
				var dnodeLi = $('#dnodeLiId');
				if (dnodeLi == null || dnodeLi == undefined) return;
				dnodeLi.show();
			}
		}	
	}
};


















