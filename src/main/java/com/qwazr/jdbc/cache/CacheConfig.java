package com.qwazr.jdbc.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class CacheConfig {
	private List<Pattern> patterns = new ArrayList<>();

	public CacheConfig() {
	}

	public CacheConfig(List<String> patternStrs) {
		for (String p : patternStrs) {
			patterns.add(Pattern.compile(p, CASE_INSENSITIVE));
		}
	}

	public static CacheConfig alwaysAccept() {
		return new CacheConfig() {
			@Override
			public boolean acceptQuery(String query) {
				return true;
			}
		};
	}

	public boolean acceptQuery(String query) {
		for (Pattern pattern : patterns) {
			Matcher matcher = pattern.matcher(query);
			if (matcher.find()) {
				System.out.println("accept [" + query + "]");
				return true;
			}
		}
		System.out.println("reject [" + query + "]");
		return false;
	}
}
