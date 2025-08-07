export default {
  register: {
    nameError: "No special characters are allowed",
    title: 'TDengine Management System',
    titleTip: 'Please register first to use TDengine explorer for better experience and technical support',
    name: 'Name',
    firstName: 'First Name',
    lastName: 'Last Name',
    phone: 'Mobile phone number',
    email: 'Email',
    verificationCode: 'Verification code',
    getVerificationCode: 'Get verification code',
    regetVerificationCode: 'Resend verification code',
    signin: 'Submit',
    imageVerificationCode: 'Image Verification code',
    nameTips: "Please enter your name, maximum of 80 characters",
    firstnameTips: "Please enter first name",
    lastnameTips: "Please enter last name",
    phoneTips: "Please enter email",
    emailTips: "Please enter email",
    verificationCodeTips: "Please enter the verification code",
    requirement: "The explorer must be able to connect to the Internet when you register, otherwise it will fail. After successful registration, the explorer can be used on the intranet without connecting to the Internet. For subsequent login, please use the database username and password to log in.",
    errors: {
      "verificationCodeNone": "You have not yet obtained the verification code. Please obtain the verification code again",
      "verificationCodeError": "Verification code error, please re-enter",
      "captchaInputError": "captcha input error, please re-enter",
      "verificationCodeInputError": "Verification code error, please re-enter",
      "network": "The explorer server cannot access the Internet.",
    },
    success: {
      "verificationCodeSend": "The verification code has been sent successfully.",
      "registerSuccess": "Registration successful, please log in using the database account",
    }
  }
}
