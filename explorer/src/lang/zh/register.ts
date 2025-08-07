export default {
  register: {
    nameError: "名称中不允许有特殊字符，长度为 4~32",
    titleTip: '为了提供更好的技术支持，请您在使用 TDengine 图形化管理系统前先行注册',
    title: 'TDengine 管理系统',
    name: '姓名',
    phone: '手机号',
    email: '邮箱',
    verificationCode: '验证码',
    getVerificationCode: '获取验证码',
    regetVerificationCode: '重新获取验证码',
    imageVerificationCode: '图形验证码',
    signin: '提交',
    nameTips: "请输入姓名，至少 2 个字符，最多 80 个字符",
    phoneTips: "请输入手机号，只支持中国大陆手机号码",
    emailTips: "请输入邮箱",
    verificationCodeTips: "请输入验证码",
    requirement: "注册过程必须保证 explorer 可连接互联网，否则无法注册成功。注册成功后，可内网使用，无需再连接互联网。后续登录，请使用数据库用户名密码登录。",
    errors: {
      "verificationCodeNone": "您还未获取验证码，请重新获取验证码",
      "verificationCodeError": "验证码错误，请重新输入",
      "captchaInputError": "图形验证码错误，请重新输入",
      "verificationCodeInputError": "验证码错误，请重新输入",
      "network": "explorer server 所在服务器无法访问互联网，请确认",
    },
    success: {
      "verificationCodeSend": "验证码发送成功",
      "registerSuccess": "注册成功，请使用数据库用户名密码登录",
    }
  }
}
