package com.hivemq.extension;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.auth.SimpleAuthenticator;
import com.hivemq.extension.sdk.api.auth.parameter.SimpleAuthInput;
import com.hivemq.extension.sdk.api.auth.parameter.SimpleAuthOutput;
import com.hivemq.extension.sdk.api.packets.connect.ConnackReasonCode;
import com.hivemq.extension.sdk.api.packets.connect.ConnectPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * 功能描述：授权传统用户密码授权
 *
 * @Author: fang xinliang
 * @Date: 2020/10/7 20:08
 */
public class TDTestSimpleAuthenticator implements SimpleAuthenticator {
	private static final @NotNull Logger log = LoggerFactory.getLogger(TDTestSimpleAuthenticator.class);

	private final Map<String, String> usernamePasswordMap;

	public TDTestSimpleAuthenticator(Map<String, String> usernamePasswordMap) {
		this.usernamePasswordMap = usernamePasswordMap;
	}

	@Override
	public void onConnect(SimpleAuthInput input, SimpleAuthOutput output) {

		//get connect packet from input object
		final ConnectPacket connectPacket = input.getConnectPacket();

		//validate the username and password combination
		if (validate(connectPacket.getUserName(), connectPacket.getPassword())) {
            log.info("validate the username {} and password {} combination",connectPacket.getUserName(),connectPacket.getPassword());
			//successful authentication if username and password are correct
			output.authenticateSuccessfully();

		} else {
            log.info("failed authenticattion if username or password are incorrect");
			//failed authenticattion if username or password are incorrect
			output.failAuthentication(ConnackReasonCode.BAD_USER_NAME_OR_PASSWORD, "wrong password");
		}
	}

	private boolean validate(Optional<String> usernameOptional, Optional<ByteBuffer> passwordOptional) {

		//if no username or password is set, valdation fails
		if(!usernameOptional.isPresent() || !passwordOptional.isPresent()){
			return false;
		}

		final String username = usernameOptional.get();
		final byte[] bytes = getBytesFromBuffer(passwordOptional.get());
		final String password = new String(bytes, StandardCharsets.UTF_8);

		//get password for username from map.
		final String passwordFromMap = usernamePasswordMap.get(username);

		//return true if passwords are equal, else false
		return password.equals(passwordFromMap);
	}

	private byte[] getBytesFromBuffer(ByteBuffer byteBuffer) {
		final byte[] bytes = new byte[byteBuffer.remaining()];
		byteBuffer.get(bytes);
		return bytes;
	}

}
