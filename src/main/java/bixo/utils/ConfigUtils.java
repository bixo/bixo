package bixo.utils;

import bixo.config.UserAgent;

public class ConfigUtils {

	// User agent for when we're making external requests during integration tests.
	public static final UserAgent BIXO_IT_AGENT = new UserAgent("Bixo integration test", "bixo-dev@groups.yahoo.com", "http://wiki.github.com/bixo/bixo/bixocrawler");

	// User agent when we're doing ad hoc requests using tools
	public static final UserAgent BIXO_TOOL_AGENT = new UserAgent("Bixo test tool", "bixo-dev@groups.yahoo.com", "http://wiki.github.com/bixo/bixo/bixocrawler");
	
	// User agent for when we're not doing external fetching, so we just need a fake name.
	public static final UserAgent BIXO_TEST_AGENT = new UserAgent("test", "test@domain.com", "http://test.domain.com");
}
