package com.summarizerwork.service;

import com.summarizerwork.service.service.ContentFetcherService;
import com.summarizerwork.service.service.GeminiService;

public class TestClass {
	public static void main(String[] args) {
		TestClass tc = new TestClass();
		tc.testGeminiAPI();
	}

	public void testGeminiAPI() {
		ContentFetcherService contentFetcherService = new ContentFetcherService();
		GeminiService geminiService = new GeminiService();
		String content = contentFetcherService.fetchContent("URL", "https://en.wikipedia.org/wiki/Ratan_Tata", null);
		String summary = geminiService.summarize(content);

		System.out.println(summary);
	}
}
