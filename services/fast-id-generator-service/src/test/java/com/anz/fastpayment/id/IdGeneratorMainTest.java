package com.anz.fastpayment.id;

import org.junit.jupiter.api.Test;

class IdGeneratorMainTest {
    @Test
    void main_invocation_covers_bootstrap_path() {
        // We do not start the full Spring context here; just ensure method is callable.
        IdGeneratorApplication.main(new String[] {"--spring.main.web-application-type=none"});
    }
}
