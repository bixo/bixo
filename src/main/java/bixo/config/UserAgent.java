package bixo.config;

import java.io.Serializable;

@SuppressWarnings("serial")
public class UserAgent implements Serializable {
	public static final String DEFAULT_BROWSER_VERSION = "Mozilla/5.0";
	
	private final String _agentName;
	private final String _emailAddress;
	private final String _webAddress;
	private final String _browserVersion;
	
	public UserAgent(String agentName, String emailAddress, String webAddress,
			String browserVersion) {
		_agentName = agentName;
		_emailAddress = emailAddress;
		_webAddress = webAddress;
		_browserVersion = browserVersion;
	}
	
	public UserAgent(String agentName, String emailAddress, String webAddress) {
		this(agentName, emailAddress, webAddress, DEFAULT_BROWSER_VERSION);
	}

	public String getAgentName() {
		return _agentName;
	}

	public String getUserAgentString() {
		// Mozilla/5.0 (compatible; mycrawler; +http://www.mydomain.com; mycrawler@mydomain.com)
		return String.format("%s (compatible; %s; +%s; %s)",
				_browserVersion, getAgentName(), _webAddress, _emailAddress);
	}
}
