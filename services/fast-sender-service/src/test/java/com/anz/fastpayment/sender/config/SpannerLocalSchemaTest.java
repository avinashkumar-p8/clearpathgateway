package com.anz.fastpayment.sender.config;

import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class SpannerLocalSchemaTest {
	@Test
	void ensureTables_createsTable() {
		SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
		SpannerLocalSchema s = new SpannerLocalSchema(admin);
		s.ensureTables();
		verify(admin, atLeastOnce()).executeDdlStrings(anyList(), eq(true));
	}

	@Test
	void ensureTables_alreadyExists_isIgnored() {
		SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
		SpannerException se = mock(SpannerException.class);
		when(se.getErrorCode()).thenReturn(ErrorCode.ALREADY_EXISTS);
		doThrow(se).when(admin).executeDdlStrings(anyList(), eq(true));
		SpannerLocalSchema s = new SpannerLocalSchema(admin);
		s.ensureTables();
		verify(admin, atLeastOnce()).executeDdlStrings(anyList(), eq(true));
	}
}
