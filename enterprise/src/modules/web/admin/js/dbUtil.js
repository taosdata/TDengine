/**
 * handle error
 * and some dialog like error or confirm
 **/

DB_CODE_UNKNOWN                 = 0;
DB_CODE_SERVER_NULL             = 1;
DB_CODE_NULL_USER_OR_PWD        = 2;
DB_CODE_INVALID_SESSION         = 3;
DB_CODE_INVALID_USER            = 4;
DB_CODE_INVALID_FORMAT          = 5;
DB_CODE_NO_DATAS                = 6;
DB_CODE_CONFIRM                 = 7;
DB_CODE_EXEC_ERROR              = 8;
DB_CODE_TABLE_NAME_NULL         = 9;
DB_CODE_TABLE_MAX_COLUMNS       = 10;
DB_CODE_TABLE_MIN_COLUMNS       = 11;
DB_CODE_TABLE_FIRSTKEY_NULL     = 12;
DB_CODE_TABLE_COL_TYPE_NULL     = 13;
DB_CODE_TABLE_COL_NAME_NULL     = 14;
DB_CODE_TABLE_COL_BINARY_NULL   = 15;
DB_CODE_TABLE_COL_NUM_ZERO      = 16;
DB_CODE_CACHE_INVALID_ROW       = 17;
DB_CODE_INVALID_SERVER          = 18;
DB_CODE_INVALID_SQL             = 19;
DB_CODE_EMPTY_SQL               = 20;
DB_CODE_DB_NOT_SELECT           = 21;
DB_CODE_DB_NOT_EXIST            = 22;
DB_CODE_EMPTY_DATA              = 23;

var DbUtil = new function() {

	this.ErrorMsg = {
		0 : "unknown error",
		1 : "taosd is not running",
		2 : "user name and password can't be null.",
		3 : "invalid session",
		4 : "invalid user",
		5 : "server return invalid json format",
		6 : "no data return from server",
		7 : "please confirm the action, it couldn't be recovered",
		8 : "execute error, affect rows is 0",
		9 : "input table name is null",
		10: "column number must be larger then 32",
		11: "column number must be larger then 1",
		12: "first column name can't be null",
		13: "data type can't be null",
		14: "data name can't be null",
		15: "binary column length should large then 0, such as col_name(10)",
		16: "column num must large then 1",
		17: "cache rows is not valid",
		18: "server is not running",
		19: "invalid sql format",
		20: "empty sql command",
		21: "db not selected",
		22: "db not exist",
		23: "result set is null",
	};

	this.ErrorDialog = function(msg) 
	{
		$('#infoType').text("Error")
		$('#infoContent').text(msg);
		$('#infoDialog').modal('show')
	}

	this.SuccessDialog = function(msg) 
	{
		$('#infoType').text("Success")
		$('#infoContent').text(msg);
		$('#infoDialog').modal('show')
	}

	this.ConfirmDialog = function(code, para)
	{
		$('#confirmContent').html(para + "<br>" + this.ErrorMsg[code]);
		$('#confirmDialog').modal('show');
	}

	this.Error = function(errno) 
	{
		var msg = this.ErrorMsg[errno];
		this.ErrorDialog(msg);
	}

	this.ErrorWithMsg = function(errno, msg)
	{
		var msgInfo = this.ErrorMsg[errno] + ", code: " + msg.status + ", msg: " + msg.statusText;	
		this.ErrorDialog(msgInfo);
	}

	this.Success = function(code)
	{
		var msg = this.ErrorMsg[code];
		this.SuccessDialog(msg);
	}

	this.HandleError = function(d) 
	{
		if (d.code == 210) {	//invalid jwt
			this.ErrorDialog(d.desc);	
			setTimeout(
				function() { 
					DbUtil.GotoPage("login.html");
				}
				, 2000
			);
		}
		if (d.code == 1) { //inprogress
			this.ErrorDialog("login killed by server, login again");
		}
		else if (d.code == 42) {	//disconnect 
			this.ErrorDialog(d.desc);
		}
		//else if (d.errno == 10 || d.code == 10) {
		//	DbUtil.Error(DB_CODE_NO_DATAS);
		//}
		else {
			this.ErrorDialog(d.desc);
		}
	}
	
	this.Log = function(title, msg)
	{
		if (msg == undefined || msg == null)
			console.log(title);
		else
			console.log(title + ' - ' + msg);
	};  

	this.GotoPage = function(href)
	{
		window.location.href = href;
	}

	this.GetUrlFirstPara = function()
	{
		var url = window.location.search; //获取url中"?"符后的字串   
		
		if (url.indexOf("=") != -1) {   
			strs = url.split("=");   
			if (strs.length != 2) {
				return "";
			}
			return strs[1];
		}   
		return ""; 
	}

	this.CheckName = function(str, desc)
	{
		if (str == null || str == "") {
			DbUtil.ErrorDialog(desc + " can't be null");
			return false;
		}
		
		var first = str[0];
		var reg = /^[A-Za-z]+$/;
		if (!reg.test(first)){  
			DbUtil.ErrorDialog(desc + " should start from [ a-z ] [ A-Z ]");
			return false;   
		} 
			
		reg = /^[\u4e00-\u9fa5A-Za-z0-9-_]*$/;
		if (!reg.test(str)){  
			DbUtil.ErrorDialog(desc + " should in this range [ 0-9 ] [ a-z ] [ A-Z ] [ _- ]");
			return false;   
		}    
		
		return true;
	}

	this.CheckIP = function(str, desc)
	{
		if (str == null || str == "") {
			DbUtil.ErrorDialog(desc + " can't be null");
			return false;
		}
		
		var reg = /^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])(\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])){3}$/;
		if (!reg.test(str)){  
			DbUtil.ErrorDialog(desc + " invalid format");
			return false;   
		}    
		
		return true;
	}
};
























