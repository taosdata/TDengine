package com.hivemq.extension;

import com.hivemq.extension.sdk.api.auth.Authenticator;
import com.hivemq.extension.sdk.api.auth.parameter.AuthenticatorProviderInput;
import com.hivemq.extension.sdk.api.services.auth.provider.AuthenticatorProvider;

import java.util.Map;

/**
 * 功能描述：
 *
 * @Author: fang xinliang
 * @Date: 2020/10/7 20:35
 */
public class TDTestAuthenticatorProvider implements AuthenticatorProvider {
	private final Map<String, String> usernamePasswordMap;

	public TDTestAuthenticatorProvider(Map<String, String> usernamePasswordMap) {
		this.usernamePasswordMap = usernamePasswordMap;
	}

	@Override
	public Authenticator getAuthenticator(AuthenticatorProviderInput authenticatorProviderInput) {

		//A new instance must be returned, because the authenticator has state.
		return new TDTestSimpleAuthenticator(usernamePasswordMap);
	}
}
