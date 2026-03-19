package com.summarizerapi.service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CachedJob {
	private String jobId;
	private String status;
	private String contentHash;
	private String originalUrl;
	private String inputText;
	private String summary;
	private String errorMessage;
	private Boolean cached;
	private Long processingTimeMs;
	private String createdAt;
	private String updatedAt;
}
