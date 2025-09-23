import { expect } from '@playwright/test';
import assert from 'assert';
import quotedPrintable from 'quoted-printable';
import utf8 from 'utf8';
import Imap from 'imap';
// import { inspect } from 'util';

const globalConfig = {
  username: process.env.LOGIN_USERNAME || 'cloudci@taosdata.com',
  password: process.env.EMAIL_LOGIN_PASSWORD || 'Tbase125!'
};

/**
 * 邮件内容如下：
 * <p>Hi Chong,</p><p>We received your request to reset your TDengine Cloud password.</p><p><a target="_blank" rel="noopener noreferrer nofollow" href="https://cloud.tdengine.com/reset?code=UGRBRVpKaHVHSGFrSGlyaG42Mmdna0xFUkJLcWwva0dNNUsraDI0VXZCNE1oaU4rMFB1Z0J0NUptYmI2VUZXZUxZc0E0a2tSKzNGNkZxN2VGdldSWEx5NExBQjBMSm1sd3lXdVNxcnpoaWIxVlkxYi9GS25pRDRCaDYzb0VNLzA%3D">Click here to reset your password.</a></p><p>This link is valid for 24 hours.</p><p>Note: If you did not request a password reset, please disregard this message.</p><p>Regards,</p><p>The TDengine Team</p>
 */
export async function get_reset_password_link(): Promise<string> {
  const email_content = await get_newest_email();
  expect(email_content).toContain('TDengine Support <support@tdengine.com>');
  const regex = /href="([^"]+)">Click here to reset your password./;
  const match = email_content.match(regex);
  assert.notStrictEqual(match, null, 'get_reset_password_link 不为 null');
  return match![1];
}

export async function get_activate_account_link(
  username: string = globalConfig.username,
  password: string = globalConfig.password
): Promise<string> {
  const email_content = await get_newest_email(username, password);
  expect(email_content).toContain('TDengine Support <support@tdengine.com>');
  const regex = /href="([^"]+)">Click here to activate your account and get started./;
  const match = email_content.match(regex);
  assert.notStrictEqual(match, null, 'get_activate_account_link 不为 null');
  return match![1];
}

// const simpleParser = require('mailparser').simpleParser;

export async function getEmailVerificationCode(
  username: string = globalConfig.username,
  password: string = globalConfig.password,
  sendStart: Date
): Promise<string> {
  let titleReg: string;
  if (process.env.TEST_LANGUAGE === 'en') {
    titleReg = 'Your TDengine IDMP account verification code';
  } else {
    titleReg = '激活验证码';
  }
  const email_content = await getEmailBySubjectRegex(titleReg, username, password, 'support@taosdata.com', sendStart);
  // expect(email_content).toContain('TDengine Support');
  console.log('Email content:', email_content);
  let match: RegExpMatchArray | null = null;
  if (process.env.TEST_LANGUAGE === 'en') {
    match = email_content.match(/(Your verification code is: ).*(\d{6}).*/);
    assert.notStrictEqual(match, undefined, 'get_email_verification_code 不为 undefined');
    assert.notStrictEqual(match, null, 'get_email_verification_code 不为 null');
    return Promise.resolve(match![2]);
  }
  if (process.env.TEST_LANGUAGE === 'zh') {
    match = email_content.match(/您的 TDengine IDMP 验证码是：<span style="font-weight: bold">(\d{6})<\/span>/);
    assert.notStrictEqual(match, null, 'get_email_verification_code 不为 null');
    assert.notStrictEqual(match, undefined, 'get_email_verification_code 不为 undefined');
    return Promise.resolve(match![1]);
  }
  return Promise.reject('Not found the verification mail');
}

async function get_newest_email(
  username: string = globalConfig.username,
  password: string = globalConfig.password
): Promise<string> {
  return new Promise(function (resolve, reject) {
    const imap = new Imap({
      user: username,
      password: password,
      host: 'imap.exmail.qq.com',
      port: 993,
      tls: true
    });

    imap.once('ready', function () {
      imap.openBox('INBOX', true, function (err: Error | null, box: Imap.Box) {
        if (err) {
          imap.end();
          reject(err);
          return;
        }
        const fetch = imap.seq.fetch(box.messages.total + ':*', {
          bodies: ['HEADER.FIELDS (FROM TO SUBJECT DATE)', 'TEXT']
        });
        fetch.on('message', function (msg: Imap.ImapMessage) {
          let body = '';
          msg.on('body', function (stream: NodeJS.ReadableStream) {
            stream.on('data', function (chunk) {
              body += chunk.toString('utf8');
            });
            // stream.once('end', function () {
            //     console.log(prefix + 'Parsed header: %s', inspect(Imap.parseHeader(buffer)));
            // });
          });
          msg.once('end', function () {
            imap.end();
            const decodedText = utf8.decode(quotedPrintable.decode(body));
            console.log('Email body:', decodedText);
            resolve(decodedText);
          });
        });
        fetch.once('error', function (err: Error) {
          console.log('Fetch error: ' + err);
          imap.end();
        });
        fetch.once('end', function () {
          console.log('Done fetching all messages!');
          imap.end();
        });
      });
    });

    imap.once('error', function (err: Error) {
      console.log(err);
      imap.end();
      reject(err);
    });

    imap.once('end', function () {
      console.log('IMAP Connection ended');
    });

    imap.connect();
  });
}

function formatDateToRFC2822(date: Date): string {
  // 获取类似 "Wed, 02 Jul 2025 07:12:24 GMT" 的格式
  const utcString = date.toUTCString();

  // 获取时区偏移量
  const tzOffset = -date.getTimezoneOffset();
  const tzOffsetHours = Math.floor(Math.abs(tzOffset) / 60)
    .toString()
    .padStart(2, '0');
  const tzOffsetMinutes = (Math.abs(tzOffset) % 60).toString().padStart(2, '0');
  const tzOffsetSign = tzOffset >= 0 ? '+' : '-';

  // 替换末尾的 GMT 为时区偏移量
  return utcString.replace(' GMT', ` ${tzOffsetSign}${tzOffsetHours}${tzOffsetMinutes}`);
}
// function formatDateForIMAP(dateObj: Date) {
//   // 验证日期有效性
//   if (isNaN(dateObj.getTime())) {
//     throw new Error('无效的日期格式');
//   }

//   const year = dateObj.getFullYear();
//   const month = String(dateObj.getMonth() + 1).padStart(2, '0'); // 月份从0开始
//   const day = String(dateObj.getDate()).padStart(2, '0');

//   return `${year}-${month}-${day}`;
// }
/**
 * 获取主题匹配正则表达式的最新邮件
 * @param subjectRegex - 匹配邮件主题的正则表达式
 * @param username - 邮箱用户名
 * @param password - 邮箱密码
 * @returns 匹配的邮件内容
 */
export async function getEmailBySubjectRegex(
  subject: string,
  username: string = globalConfig.username,
  password: string = globalConfig.password,
  sendEmail: string = 'support@taosdata.com',
  since?: Date
): Promise<string> {
  return new Promise(function (resolve, reject) {
    const imap = new Imap({
      user: username,
      password: password,
      host: 'imap.exmail.qq.com',
      port: 993,
      tls: true
    });

    imap.once('ready', function () {
      // 打开收件箱
      imap.openBox('INBOX', true, function (err: Error | null) {
        if (err) {
          imap.end();
          reject(err);
          return;
        }
        const sinceDate = since ? formatDateToRFC2822(since) : new Date(Date.now() - 30 * 1000);
        const searchCriteria = [
          ['HEADER', 'SUBJECT', subject],
          ['SINCE', sinceDate],
          ['FROM', sendEmail]
        ];
        imap.search(searchCriteria, function (err, results) {
          if (err) {
            imap.end();
            reject(err);
            return;
          }

          console.log(
            `找到 ${username} 的 ${results.length} 封 ${sinceDate} 日期（时间是 ${since} 时间戳是 ${since?.getTime()}）以后标题为“${subject}”的邮件`
          );
          if (results.length === 0) {
            imap.end();
            reject(new Error(`没有找到从 ${sinceDate} 开始收到的邮件`));
            return;
          }
          const result = results[results.length - 1];
          const fetch = imap.fetch(result, {
            bodies: ['HEADER.FIELDS (FROM TO SUBJECT DATE)', 'TEXT']
          });

          let foundEmail = false;
          fetch.on('message', function (msg: Imap.ImapMessage) {
            let header = '';
            let body = '';
            let subject = '';
            // 获取邮件头部
            msg.on('body', function (stream: NodeJS.ReadableStream, info) {
              if (info.which === 'HEADER.FIELDS (FROM TO SUBJECT DATE)') {
                stream.on('data', function (chunk) {
                  header += chunk.toString('utf8');
                });
                stream.once('end', function () {
                  // 解析头部，获取主题
                  const parsedHeader = Imap.parseHeader(header);
                  subject = (parsedHeader.subject && parsedHeader.subject[0]) || '';
                  const sentDate = parsedHeader.date?.[0]; // RFC2822格式的日期字符串
                  const mailDate = sentDate ? new Date(sentDate) : null;
                  console.log(`邮件发送时间: ${mailDate} 时间戳是：${mailDate?.getTime()}`, mailDate);
                  // 检查主题是否匹配正则表达式
                  if (mailDate && since && since.getTime() < mailDate.getTime()) {
                    foundEmail = true;
                    console.log(`Found matching email with subject: ${subject} with date time ${mailDate}`);
                  }
                });
              } else {
                stream.on('data', function (chunk) {
                  body += chunk.toString('utf8');
                });
              }
            });

            // 当邮件处理完毕时
            msg.once('end', function () {
              if (foundEmail) {
                const decodedText = utf8.decode(quotedPrintable.decode(header + body));
                // 找到匹配邮件后立即结束连接
                imap.end();
                resolve(decodedText);
              }
            });
          });

          // 搜索结束后处理结果
          fetch.once('error', function (err: Error) {
            console.log('Fetch error: ' + err);
            imap.end();
            reject(err);
          });

          fetch.once('end', function () {
            if (!foundEmail) {
              imap.end();
              reject(new Error('邮件内容获取失败'));
            }
            console.log('Done fetching message');
          });
        });
      });
    });

    imap.once('error', function (err: Error) {
      console.log(err);
      imap.end();
      reject(err);
    });

    imap.once('end', function () {
      console.log('IMAP Connection ended');
    });

    imap.connect();
  });
}
