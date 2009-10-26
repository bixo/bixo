package bixo.utils;

import bixo.config.UserAgent;

public class ConfigUtils {

	public static final UserAgent BIXO_TEST_AGENT = new UserAgent("bixo integration test", "bixo-dev@groups.yahoo.com", "http://bixo.101tec.com");

	// User agent for when we're not doing external fetching, so we just need a fake name.
	public static final UserAgent BIXO_FAKE_AGENT = new UserAgent("test", "test@domain.com", "http://test.domain.com");
}
