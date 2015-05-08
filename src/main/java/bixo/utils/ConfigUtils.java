/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.utils;

import bixo.config.UserAgent;

public class ConfigUtils {

	// User agent for when we're making external requests during integration tests.
	public static final UserAgent BIXO_IT_AGENT = new UserAgent("Bixo integration test", "bixo-dev@groups.yahoo.com", "http://wiki.github.com/bixo/bixo/bixocrawler");

	// User agent when we're doing ad hoc requests using tools
	public static final UserAgent BIXO_TOOL_AGENT = new UserAgent("Bixo test tool", "bixo-dev@groups.yahoo.com", "http://wiki.github.com/bixo/bixo/bixocrawler");
	
	// User agent for when we're not doing external fetching, so we just need a fake name.
	public static final UserAgent BIXO_TEST_AGENT = new UserAgent("test", "test@domain.com", "http://test.domain.com");

	private ConfigUtils() {
        // Enforce class isn't instantiated
    }
}
