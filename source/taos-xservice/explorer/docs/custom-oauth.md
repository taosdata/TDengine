# 统一用户管理接口文档

# 统一用户管理对接要求

应用系统单点登录要求按照图 1 所示的流程完成单点登录。

图 1 单点登录流程要求

应用系统单点登录流程要求主要面向应用系统和运维管理分系统。

1. 应用系统需要在用户访问应用时判断是否登录，若未登录，携带指定参数重定向至单点登录统一认证平台；
2. 应用系统提供单点登录成功回调页面，并接收参数
3. 应用系统通过 HTTP 接口换取用户信息

在应用系统单点登录开发要求应用系统按照本规范拟制的流程规范和接口规范，实现单点登录进入应用系统。

接口示例见表 1-表 8。

**表 1 第三方系统跳转 SSO 认证地址**

<table>
<tr>
<td>接口名称<br/></td><td>第三方系统跳转SSO认证地址<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>第三方系统接入SSO统一登录，第三方登录页重定向到此地址<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>/sso/oauth2.0/authorize?response_type=code&client_id={client_id}&redirect_url={redirect_url}<br/></td></tr>
<tr>
<td colspan="2">接口地址示例<br/></td></tr>
<tr>
<td colspan="2">http://192.168.1.1/sso/oauth2.0/authorize?response_type=code&client_id=NtT4ey1C&redirect_url=http://192.168.1.2/ssoLogin<br/></td></tr>
<tr>
<td colspan="2">SSO登录成功后重定向地址实例<br/></td></tr>
<tr>
<td colspan="2">http://192.168.1.2/ssoLogin?code=b87576e1-fd14-47e5-a835-0d601b18387a<br/></td></tr>
</table>

**表 2 第三方系统跳转 SSO 认证地址参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>client_id<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>应用id，应用注册时获得<br/></td></tr>
<tr>
<td>redirect_url<br/></td><td>string<br/></td><td>500<br/></td><td>是<br/></td><td>登录成功后重定向回第三方系统地址<br/></td></tr>
</table>

**表 3 SSO 登录成功后重定向回第三方地址参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>code<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>认证编码<br/></td></tr>
</table>

**表 4 token 认证接口**

<table>
<tr>
<td>接口名称<br/></td><td>token认证接口<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>根据接收到请求参数，返回token<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>sso/oauth2.0/accessToken<br/></td></tr>
<tr>
<td colspan="2">接口请求参数示例<br/></td></tr>
<tr>
<td colspan="2">{<br/>  "client_id": "1001",<br/>  "client_secret": "Xngxksq432",<br/>  "grant_type": "authorization_code",<br/>  "code": "b87576e1-fd14-47e5-a835-0d601b18387a"<br/>}<br/></td></tr>
<tr>
<td colspan="2">接口返回示例<br/></td></tr>
<tr>
<td colspan="2">{<br/>  "access_token": " b1d508f3-32e7-4d6d-ac62-87836406704c"<br/>}<br/></td></tr>
</table>

**表 5 token 认证接口请求参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>client_id<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>应用id，应用注册时获得<br/></td></tr>
<tr>
<td>client_secret<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>应用密钥，应用注册时获得<br/></td></tr>
<tr>
<td>grant_type<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>认证类型，固定常量值<br/></td></tr>
<tr>
<td>code<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>认证编码，单点登录成功后回调页面参数<br/></td></tr>
</table>

**表 8 token 认证接口返回参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>access_token<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>token值，用于换取用户信息<br/></td></tr>
</table>

**表 9 用户信息接口**

<table>
<tr>
<td>接口名称<br/></td><td>用户信息接口<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>根据接收到请求参数，返回用户信息<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>sso/oauth2.0/profile<br/></td></tr>
<tr>
<td>请求方式<br/></td><td>Get<br/></td></tr>
<tr>
<td colspan="2">接口请求参数示例<br/></td></tr>
<tr>
<td colspan="2">access_token=b1d508f3-32e7-4d6d-ac62-87836406704c<br/></td></tr>
<tr>
<td colspan="2">接口返回示例<br/></td></tr>
<tr>
<td colspan="2">{<br/>  "username": "admin",<br/>  "attributes": {<br/>  "token_expired": 7200,<br/>  "token_time": 1638253419364,<br/>  "roles": [{<br/>      "role_name":"管理员"<br/>}],<br/>  "orgs": [{<br/>      "org_name":"xx部门 ",    <br/>      "org_path":"/xx总部/xx中心/xx部门"<br/>}],<br/>}<br/>}<br/></td></tr>
</table>

**表 10 用户信息接口请求参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>access_token<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>token值，token认证接口返回<br/></td></tr>
</table>

**表 11 用户信息接口返回参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>username<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户名<br/></td></tr>
<tr>
<td>attributes->token_expire<br/></td><td>int<br/></td><td>11<br/></td><td>是<br/></td><td>token有效时间<br/></td></tr>
<tr>
<td>attributes->token_time<br/></td><td>int<br/></td><td>11<br/></td><td>是<br/></td><td>token生成时间<br/></td></tr>
<tr>
<td>roles->role_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户所拥有的角色名<br/></td></tr>
<tr>
<td>orgs->org_name<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>用户所在组织<br/></td></tr>
<tr>
<td>orgs->org_path<br/></td><td>string<br/></td><td>255<br/></td><td>否<br/></td><td>用户所在组织层级关系<br/></td></tr>
</table>

**表 12 同步信息前置获取 token 接口**

<table>
<tr>
<td>接口名称<br/></td><td>用户信息接口<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>根据接收到请求参数，返回用户信息<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>/rest/v1/sso/userLogin/login<br/></td></tr>
<tr>
<td>请求方式<br/></td><td>Post<br/></td></tr>
<tr>
<td colspan="2">接口请求参数示例<br/></td></tr>
<tr>
<td colspan="2">{<br/>    "username": "xxxx",<br/>    "password": "xxxx"<br/>}<br/></td></tr>
<tr>
<td colspan="2">接口返回示例<br/></td></tr>
<tr>
<td colspan="2">{<br/>    "success": true,<br/>    "code": 200,<br/>    "data": {<br/>        "access_token": "eyJhbGciOiJSUzI1NiJ9.d4b32c91697238fb140551d6db9351822a047fa6b<br/>c7a0ffd2e5f7bea77b8468ced2f6c063034cf3e07a2f495814fa203fc242c331c5def423d1f5cbb9539838<br/>aa29a8156779eaed894bc79d4ff81ecbf.l4zDWtl8drMDyDvNSDxmBvMH73gBLJJ5mdZgqlUV-jKT3o7RwkBGk<br/>TdDahZVaMmD7oBRuT6qERO3zr2nroMP-_77gwP1zeQ3_tsaQA2wCcfP094x-plzlJDCTOU_33WuNNIHhqDQAuOh<br/>31QeTC7V8wN7qp-fZDWdtT07AcJpfhKSiHcs4Alqmhe4mT18P9LsAMZjiH5mQ24QKSqyOdV-Xtxe3lCESW9QA0B<br/>RKgMLdKaEkG722GxPs7G61hwU2LcGhey778_qb7sHCKsYUnzD0scigXX9G1WiN0KwqSAvr0mknVqvhlFCiXEjCh<br/>OmRXnvuEXWfeVJovx4rADQXwyJVw"<br/>    }<br/>}<br/></td></tr>
</table>

**表 13 同步信息前置获取 token 接口请求参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>username<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户名<br/></td></tr>
<tr>
<td>password<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户密码<br/></td></tr>
</table>

**表 14 同步信息前置获取 token 接口参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>success<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>请求成功表示<br/></td></tr>
<tr>
<td>code<br/></td><td>int<br/></td><td>4<br/></td><td>是<br/></td><td>状态码<br/></td></tr>
<tr>
<td>data->access_token<br/></td><td>string<br/></td><td>255<br/></td><td>是<br/></td><td>请求成功返回的access_token<br/></td></tr>
</table>

**表 15 同步用户信息接口**

<table>
<tr>
<td>接口名称<br/></td><td>同步用户信息接口<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>根据接收到请求参数，返回用户信息<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>/sso/oauth2.0/getUsers<br/></td></tr>
<tr>
<td>请求方式<br/></td><td>Get<br/></td></tr>
<tr>
<td colspan="2">接口请求参数示例<br/></td></tr>
<tr>
<td colspan="2">--header 'Access_token: eyJhbGciOiJSUzI1NiJ9.d4b32c91697238fb140551d6db9351822a047fa6bc7a0ffd2e5f7bea<br/>77b8468c1da88c2871afb09f350e3961d001af605b3f4d7cc420586726da4b11aefb44420cf06a072f7b54eb1959bae682da5<br/>c10.Wg1HVq1S28f3ja83keAF3HcWUm0wL-vUSkhYEz9mAaentzm8rK5B6tfqU18NaVAMkYZ7_zyLbqZm8hvYgUTvq_n0f7GoP_FU0<br/>y0zh3D5Xr0mjI9pejIFXx3iqtLlJSjYpxbcaJCHH6aXDnsnRVTYBMyUN3ca5sRjI_fjlpElfa6TJJ4nZiE-rU8KXmx7DxOg0q93ru<br/>ehLi3RVkIDo8bofp7hpWRmLgHWysXU-G2eDNItnDbAQpWG0-owt-AUb2qRAP7bMgbqbqPP6cURO7-hexs0jaxy6WMFRUCUWM6V0pr<br/>WvzNMAb-FhgACkz0yTJg642O2cq767UKZmNVAIVNaug' <br/></td></tr>
<tr>
<td colspan="2">接口返回示例<br/></td></tr>
<tr>
<td colspan="2">[<br/>    {<br/>        "user_name": "admin",<br/>        "user_display_name": "管理员",<br/>        "user_org_path": "/总部组织",<br/>        "org": {<br/>            "org_name": "总部组织",<br/>            "org_display": "总部组织",<br/>            "org_path": "/总部组织"<br/>        },<br/>        "roles": [<br/>            {<br/>                "role_name": "管理员",<br/>                "role_display_name": "管理员",<br/>                "org": {<br/>                    "org_name": "总部组织",<br/>                    "org_display": "总部组织",<br/>                    "org_path": "/总部组织"<br/>                }<br/>            }<br/>        ]<br/>    }]<br/></td></tr>
</table>

**表 16 同步用户信息接口请求参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>access_token<br/></td><td>string<br/></td><td>255<br/></td><td>是<br/></td><td>token值，用于换取用户信息<br/></td></tr>
</table>

**表 17 同步用户信息接口返回参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>user_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户名<br/></td></tr>
<tr>
<td>user_display_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户显示名称<br/></td></tr>
<tr>
<td>user_org_path<br/></td><td>string<br/></td><td>255<br/></td><td>是<br/></td><td>用户组织路径<br/></td></tr>
<tr>
<td>org->org_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>用户的组织名<br/></td></tr>
<tr>
<td>org->org_display<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>组织全称<br/></td></tr>
<tr>
<td>org->org_path<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>组织路径<br/></td></tr>
<tr>
<td>roles->role_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>角色名称<br/></td></tr>
<tr>
<td>roles->role_display_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>角色全称<br/></td></tr>
<tr>
<td>roles->org->org_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>角色的组织名称<br/></td></tr>
<tr>
<td>roles->org->org_display<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>角色的组织全称<br/></td></tr>
<tr>
<td>roles->org->org_path<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>角色的组织路径<br/></td></tr>
</table>

**表 18 同步组织信息接口**

<table>
<tr>
<td>接口名称<br/></td><td>同步组织信息接口<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>根据接收到请求参数，返回组织信息<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>/sso/oauth2.0/getOrgs<br/></td></tr>
<tr>
<td>请求方式<br/></td><td>Get<br/></td></tr>
<tr>
<td colspan="2">接口请求参数示例<br/></td></tr>
<tr>
<td colspan="2">--header 'Access_token: eyJhbGciOiJSUzI1NiJ9.d4b32c91697238fb140551d6db9351822a047fa6bc7a0ffd2e5f7bea<br/>77b8468c1da88c2871afb09f350e3961d001af605b3f4d7cc420586726da4b11aefb44420cf06a072f7b54eb1959bae682da5<br/>c10.Wg1HVq1S28f3ja83keAF3HcWUm0wL-vUSkhYEz9mAaentzm8rK5B6tfqU18NaVAMkYZ7_zyLbqZm8hvYgUTvq_n0f7GoP_FU0<br/>y0zh3D5Xr0mjI9pejIFXx3iqtLlJSjYpxbcaJCHH6aXDnsnRVTYBMyUN3ca5sRjI_fjlpElfa6TJJ4nZiE-rU8KXmx7DxOg0q93ru<br/>ehLi3RVkIDo8bofp7hpWRmLgHWysXU-G2eDNItnDbAQpWG0-owt-AUb2qRAP7bMgbqbqPP6cURO7-hexs0jaxy6WMFRUCUWM6V0pr<br/>WvzNMAb-FhgACkz0yTJg642O2cq767UKZmNVAIVNaug' <br/></td></tr>
<tr>
<td colspan="2">接口返回示例<br/></td></tr>
<tr>
<td colspan="2">[<br/>    {<br/>        "org_name": "总部组织",<br/>        "org_full_name": "总部组织",<br/>        "description": "总部组织",<br/>        "org_path": "/总部组织",<br/>        "org_city": ""<br/>}<br/>]<br/></td></tr>
</table>

**表 19 同步组织信息接口请求参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>access_token<br/></td><td>string<br/></td><td>255<br/></td><td>是<br/></td><td>token值，用于换取组织信息<br/></td></tr>
</table>

**表 20 同步组织信息接口返回参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>org_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>组织名称<br/></td></tr>
<tr>
<td>org_full_name<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>组织全称<br/></td></tr>
<tr>
<td>description<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>组织备注<br/></td></tr>
<tr>
<td>org_path<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>组织路径<br/></td></tr>
<tr>
<td>org_city<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>所在城市<br/></td></tr>
</table>

**表 34 同步角色信息接口**

<table>
<tr>
<td>接口名称<br/></td><td>同步角色信息接口<br/></td></tr>
<tr>
<td>接口描述<br/></td><td>根据接收到请求参数，返回角色信息<br/></td></tr>
<tr>
<td>接口地址<br/></td><td>/sso/oauth2.0/getRoles<br/></td></tr>
<tr>
<td>请求方式<br/></td><td>Get<br/></td></tr>
<tr>
<td colspan="2">接口请求参数示例<br/></td></tr>
<tr>
<td colspan="2">--header 'Access_token: eyJhbGciOiJSUzI1NiJ9.d4b32c91697238fb140551d6db9351822a047fa6bc7a0ffd2e5f7bea<br/>77b8468c1da88c2871afb09f350e3961d001af605b<br/>3f4d7cc420586726da4b11aefb44420cf06a072f7b54eb1959bae682da5<br/>c10.Wg1HVq1S28f3ja83keAF3HcWUm0wL-vUSkhYEz9mAaentzm8rK5B6tfqU18NaVAMkYZ7_zyLbqZm8hvYgUTvq_n0f7GoP_FU0<br/>y0zh3D5Xr0mjI9pejIFXx3iqtLlJSjYpxbcaJCHH6aXDnsnRVTYBMyUN3ca5sRjI_fjlpElfa6TJJ4nZiE-rU8KXmx7DxOg0q93ru<br/>ehLi3RVkIDo8bofp7hpWRmLgHWysXU-G2eDNItnDbAQpWG0-owt-AUb2qRAP7bMgbqbqPP6cURO7-hexs0jaxy6WMFRUCUWM6V0pr<br/>WvzNMAb-FhgACkz0yTJg642O2cq767UKZmNVAIVNaug' <br/></td></tr>
<tr>
<td colspan="2">接口返回示例<br/></td></tr>
<tr>
<td colspan="2">[<br/>    {<br/>        "role_name": "管理员",<br/>        "role_org": "总部组织",<br/>        "role_org_path": "/总部组织",<br/>        "remark": "管理员"<br/>    }<br/>]<br/></td></tr>
</table>

**表 35 同步组织信息接口请求参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>access_token<br/></td><td>string<br/></td><td>255<br/></td><td>是<br/></td><td>token值，用于换取角色信息<br/></td></tr>
</table>

**表 36 同步组织信息接口返回参数说明**

<table>
<tr>
<td>参数名<br/></td><td>参数类型<br/></td><td>字段长度<br/></td><td>是否必填<br/></td><td>说明<br/></td></tr>
<tr>
<td>role_name<br/></td><td>string<br/></td><td>50<br/></td><td>是<br/></td><td>角色名称<br/></td></tr>
<tr>
<td>role_org<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>角色组织名称<br/></td></tr>
<tr>
<td>role_org_path<br/></td><td>string<br/></td><td>255<br/></td><td>否<br/></td><td>角色组织路径<br/></td></tr>
<tr>
<td>remark<br/></td><td>string<br/></td><td>50<br/></td><td>否<br/></td><td>角色备注<br/></td></tr>
</table>
